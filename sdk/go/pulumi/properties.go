// Copyright 2016-2018, Pulumi Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// nolint: lll
package pulumi

import (
	"context"
	"reflect"
	"sync"

	"github.com/pkg/errors"
	"github.com/pulumi/pulumi/pkg/util/contract"
)

// Output helps encode the relationship between resources in a Pulumi application. Specifically an output property
// holds onto a value and the resource it came from. An output value can then be provided when constructing new
// resources, allowing that new resource to know both the value as well as the resource the value came from.  This
// allows for a precise "dependency graph" to be created, which properly tracks the relationship between resources.
type Output interface {
	ElementType() reflect.Type

	Apply(applier interface{}) Output
	ApplyWithContext(ctx context.Context, applier interface{}) Output

	getState() *OutputState
	dependencies() []Resource
	fulfill(value interface{}, known bool, err error)
	resolve(value interface{}, known bool)
	reject(err error)
	await(ctx context.Context) (interface{}, bool, error)
}

var outputType = reflect.TypeOf((*Output)(nil)).Elem()

var concreteTypeToOutputType sync.Map // map[reflect.Type]reflect.Type

// RegisterOutputType registers an Output type with the Pulumi runtime. If a value of this type's concrete type is
// returned by an Apply, the Apply will return the specific Output type.
func RegisterOutputType(output Output) {
	elementType := output.ElementType()
	existing, hasExisting := concreteTypeToOutputType.LoadOrStore(elementType, reflect.TypeOf(output))
	if hasExisting {
		panic(errors.Errorf("an output type for %v is already registered: %v", elementType, existing))
	}
}

const (
	outputPending = iota
	outputResolved
	outputRejected
)

// OutputState holds the internal details of an Output and implements the Apply and ApplyWithContext methods.
type OutputState struct {
	mutex sync.Mutex
	cond  *sync.Cond

	state uint32 // one of output{Pending,Resolved,Rejected}

	value interface{} // the value of this output if it is resolved.
	err   error       // the error associated with this output if it is rejected.
	known bool        // true if this output's value is known.

	element reflect.Type // the element type of this output.
	deps    []Resource   // the dependencies associated with this output property.
}

func (o *OutputState) elementType() reflect.Type {
	if o == nil {
		return anyType
	}
	return o.element
}

func (o *OutputState) dependencies() []Resource {
	if o == nil {
		return nil
	}
	return o.deps
}

func (o *OutputState) fulfill(value interface{}, known bool, err error) {
	if o == nil {
		return
	}

	o.mutex.Lock()
	defer func() {
		o.mutex.Unlock()
		o.cond.Broadcast()
	}()

	if o.state != outputPending {
		return
	}

	if err != nil {
		o.state, o.err, o.known = outputRejected, err, true
	} else {
		o.state, o.value, o.known = outputResolved, value, known
	}
}

func (o *OutputState) resolve(value interface{}, known bool) {
	o.fulfill(value, known, nil)
}

func (o *OutputState) reject(err error) {
	o.fulfill(nil, true, err)
}

func (o *OutputState) await(ctx context.Context) (interface{}, bool, error) {
	for {
		if o == nil {
			// If the state is nil, treat its value as resolved and unknown.
			return nil, false, nil
		}

		o.mutex.Lock()
		for o.state == outputPending {
			if ctx.Err() != nil {
				return nil, true, ctx.Err()
			}
			o.cond.Wait()
		}
		o.mutex.Unlock()

		if !o.known || o.err != nil {
			return nil, o.known, o.err
		}

		ov, ok := o.value.(Output)
		if !ok {
			return o.value, true, nil
		}
		o = ov.getState()
	}
}

func (o *OutputState) getState() *OutputState {
	return o
}

func newOutputState(elementType reflect.Type, deps ...Resource) *OutputState {
	out := &OutputState{
		element: elementType,
		deps:    deps,
	}
	out.cond = sync.NewCond(&out.mutex)
	return out
}

var outputStateType = reflect.TypeOf((*OutputState)(nil))
var outputTypeToOutputState sync.Map // map[reflect.Type]func(Output, ...Resource)

func newOutput(typ reflect.Type, deps ...Resource) Output {
	contract.Assert(typ.Implements(outputType))

	outputFieldV, ok := outputTypeToOutputState.Load(typ)
	if !ok {
		outputField := -1
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.Anonymous && f.Type == outputStateType {
				outputField = i
				break
			}
		}
		contract.Assert(outputField != -1)
		outputTypeToOutputState.Store(typ, outputField)
		outputFieldV = outputField
	}

	output := reflect.New(typ).Elem()
	state := newOutputState(output.Interface().(Output).ElementType(), deps...)
	output.Field(outputFieldV.(int)).Set(reflect.ValueOf(state))
	return output.Interface().(Output)
}

// NewOutput returns an output value that can be used to rendezvous with the production of a value or error.  The
// function returns the output itself, plus two functions: one for resolving a value, and another for rejecting with an
// error; exactly one function must be called. This acts like a promise.
func NewOutput() (AnyOutput, func(interface{}), func(error)) {
	out := newOutputState(anyType)

	resolve := func(v interface{}) {
		out.resolve(v, true)
	}
	reject := func(err error) {
		out.reject(err)
	}

	return AnyOutput{out}, resolve, reject
}

var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
var errorType = reflect.TypeOf((*error)(nil)).Elem()

func makeContextful(fn interface{}, elementType reflect.Type) interface{} {
	fv := reflect.ValueOf(fn)
	if fv.Kind() != reflect.Func {
		panic(errors.New("applier must be a function"))
	}

	ft := fv.Type()
	if ft.NumIn() != 1 || !elementType.AssignableTo(ft.In(0)) {
		panic(errors.Errorf("applier must have 1 input parameter assignable from %v", elementType))
	}

	var outs []reflect.Type
	switch ft.NumOut() {
	case 1:
		// Okay
		outs = []reflect.Type{ft.Out(0)}
	case 2:
		// Second out parameter must be of type error
		if !ft.Out(1).AssignableTo(errorType) {
			panic(errors.New("applier's second return type must be assignable to error"))
		}
		outs = []reflect.Type{ft.Out(0), ft.Out(1)}
	default:
		panic(errors.New("appplier must return exactly one or two values"))
	}

	ins := []reflect.Type{contextType, ft.In(0)}
	contextfulType := reflect.FuncOf(ins, outs, ft.IsVariadic())
	contextfulFunc := reflect.MakeFunc(contextfulType, func(args []reflect.Value) []reflect.Value {
		// Slice off the context argument and call the applier.
		return fv.Call(args[1:])
	})
	return contextfulFunc.Interface()
}

func checkApplier(fn interface{}, elementType reflect.Type) reflect.Value {
	fv := reflect.ValueOf(fn)
	if fv.Kind() != reflect.Func {
		panic(errors.New("applier must be a function"))
	}

	ft := fv.Type()
	if ft.NumIn() != 2 || !contextType.AssignableTo(ft.In(0)) || !elementType.AssignableTo(ft.In(1)) {
		panic(errors.Errorf("applier's input parameters must be assignable from %v and %v", contextType, elementType))
	}

	switch ft.NumOut() {
	case 1:
		// Okay
	case 2:
		// Second out parameter must be of type error
		if !ft.Out(1).AssignableTo(errorType) {
			panic(errors.New("applier's second return type must be assignable to error"))
		}
	default:
		panic(errors.New("appplier must return exactly one or two values"))
	}

	// Okay
	return fv
}

// Apply transforms the data of the output property using the applier func. The result remains an output
// property, and accumulates all implicated dependencies, so that resources can be properly tracked using a DAG.
// This function does not block awaiting the value; instead, it spawns a Goroutine that will await its availability.
// nolint: interfacer
func (o *OutputState) Apply(applier interface{}) Output {
	return o.ApplyWithContext(context.Background(), makeContextful(applier, o.elementType()))
}

var anyOutputType = reflect.TypeOf((*AnyOutput)(nil)).Elem()

// ApplyWithContext transforms the data of the output property using the applier func. The result remains an output
// property, and accumulates all implicated dependencies, so that resources can be properly tracked using a DAG.
// This function does not block awaiting the value; instead, it spawns a Goroutine that will await its availability.
// The provided context can be used to reject the output as canceled.
// nolint: interfacer
func (o *OutputState) ApplyWithContext(ctx context.Context, applier interface{}) Output {
	fn := checkApplier(applier, o.elementType())

	resultType := anyOutputType
	if ot, ok := concreteTypeToOutputType.Load(fn.Type().Out(0)); ok {
		resultType = ot.(reflect.Type)
	}

	result := newOutput(resultType, o.dependencies()...)
	go func() {
		v, known, err := o.await(ctx)
		if err != nil || !known {
			result.fulfill(nil, known, err)
			return
		}

		// If we have a known value, run the applier to transform it.
		results := fn.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(v)})
		if len(results) == 2 && !results[1].IsNil() {
			result.reject(results[1].Interface().(error))
			return
		}

		// Fulfill the result.
		result.fulfill(results[0].Interface(), true, nil)
	}()
	return result
}

// ApplyAny is like Apply, but returns a AnyOutput.
func (o *OutputState) ApplyAny(applier interface{}) AnyOutput {
	return o.Apply(applier).(AnyOutput)
}

// ApplyAnyWithContext is like ApplyWithContext, but returns a AnyOutput.
func (o *OutputState) ApplyAnyWithContext(ctx context.Context, applier interface{}) AnyOutput {
	return o.ApplyWithContext(ctx, applier).(AnyOutput)
}

// ApplyAnyArray is like Apply, but returns a AnyArrayOutput.
func (o *OutputState) ApplyAnyArray(applier interface{}) AnyArrayOutput {
	return o.Apply(applier).(AnyArrayOutput)
}

// ApplyAnyArrayWithContext is like ApplyWithContext, but returns a AnyArrayOutput.
func (o *OutputState) ApplyAnyArrayWithContext(ctx context.Context, applier interface{}) AnyArrayOutput {
	return o.ApplyWithContext(ctx, applier).(AnyArrayOutput)
}

// ApplyAnyMap is like Apply, but returns a AnyMapOutput.
func (o *OutputState) ApplyAnyMap(applier interface{}) AnyMapOutput {
	return o.Apply(applier).(AnyMapOutput)
}

// ApplyAnyMapWithContext is like ApplyWithContext, but returns a AnyMapOutput.
func (o *OutputState) ApplyAnyMapWithContext(ctx context.Context, applier interface{}) AnyMapOutput {
	return o.ApplyWithContext(ctx, applier).(AnyMapOutput)
}

// ApplyArchive is like Apply, but returns a ArchiveOutput.
func (o *OutputState) ApplyArchive(applier interface{}) ArchiveOutput {
	return o.Apply(applier).(ArchiveOutput)
}

// ApplyArchiveWithContext is like ApplyWithContext, but returns a ArchiveOutput.
func (o *OutputState) ApplyArchiveWithContext(ctx context.Context, applier interface{}) ArchiveOutput {
	return o.ApplyWithContext(ctx, applier).(ArchiveOutput)
}

// ApplyArchiveArray is like Apply, but returns a ArchiveArrayOutput.
func (o *OutputState) ApplyArchiveArray(applier interface{}) ArchiveArrayOutput {
	return o.Apply(applier).(ArchiveArrayOutput)
}

// ApplyArchiveArrayWithContext is like ApplyWithContext, but returns a ArchiveArrayOutput.
func (o *OutputState) ApplyArchiveArrayWithContext(ctx context.Context, applier interface{}) ArchiveArrayOutput {
	return o.ApplyWithContext(ctx, applier).(ArchiveArrayOutput)
}

// ApplyArchiveMap is like Apply, but returns a ArchiveMapOutput.
func (o *OutputState) ApplyArchiveMap(applier interface{}) ArchiveMapOutput {
	return o.Apply(applier).(ArchiveMapOutput)
}

// ApplyArchiveMapWithContext is like ApplyWithContext, but returns a ArchiveMapOutput.
func (o *OutputState) ApplyArchiveMapWithContext(ctx context.Context, applier interface{}) ArchiveMapOutput {
	return o.ApplyWithContext(ctx, applier).(ArchiveMapOutput)
}

// ApplyAsset is like Apply, but returns a AssetOutput.
func (o *OutputState) ApplyAsset(applier interface{}) AssetOutput {
	return o.Apply(applier).(AssetOutput)
}

// ApplyAssetWithContext is like ApplyWithContext, but returns a AssetOutput.
func (o *OutputState) ApplyAssetWithContext(ctx context.Context, applier interface{}) AssetOutput {
	return o.ApplyWithContext(ctx, applier).(AssetOutput)
}

// ApplyAssetArray is like Apply, but returns a AssetArrayOutput.
func (o *OutputState) ApplyAssetArray(applier interface{}) AssetArrayOutput {
	return o.Apply(applier).(AssetArrayOutput)
}

// ApplyAssetArrayWithContext is like ApplyWithContext, but returns a AssetArrayOutput.
func (o *OutputState) ApplyAssetArrayWithContext(ctx context.Context, applier interface{}) AssetArrayOutput {
	return o.ApplyWithContext(ctx, applier).(AssetArrayOutput)
}

// ApplyAssetMap is like Apply, but returns a AssetMapOutput.
func (o *OutputState) ApplyAssetMap(applier interface{}) AssetMapOutput {
	return o.Apply(applier).(AssetMapOutput)
}

// ApplyAssetMapWithContext is like ApplyWithContext, but returns a AssetMapOutput.
func (o *OutputState) ApplyAssetMapWithContext(ctx context.Context, applier interface{}) AssetMapOutput {
	return o.ApplyWithContext(ctx, applier).(AssetMapOutput)
}

// ApplyAssetOrArchive is like Apply, but returns a AssetOrArchiveOutput.
func (o *OutputState) ApplyAssetOrArchive(applier interface{}) AssetOrArchiveOutput {
	return o.Apply(applier).(AssetOrArchiveOutput)
}

// ApplyAssetOrArchiveWithContext is like ApplyWithContext, but returns a AssetOrArchiveOutput.
func (o *OutputState) ApplyAssetOrArchiveWithContext(ctx context.Context, applier interface{}) AssetOrArchiveOutput {
	return o.ApplyWithContext(ctx, applier).(AssetOrArchiveOutput)
}

// ApplyAssetOrArchiveArray is like Apply, but returns a AssetOrArchiveArrayOutput.
func (o *OutputState) ApplyAssetOrArchiveArray(applier interface{}) AssetOrArchiveArrayOutput {
	return o.Apply(applier).(AssetOrArchiveArrayOutput)
}

// ApplyAssetOrArchiveArrayWithContext is like ApplyWithContext, but returns a AssetOrArchiveArrayOutput.
func (o *OutputState) ApplyAssetOrArchiveArrayWithContext(ctx context.Context, applier interface{}) AssetOrArchiveArrayOutput {
	return o.ApplyWithContext(ctx, applier).(AssetOrArchiveArrayOutput)
}

// ApplyAssetOrArchiveMap is like Apply, but returns a AssetOrArchiveMapOutput.
func (o *OutputState) ApplyAssetOrArchiveMap(applier interface{}) AssetOrArchiveMapOutput {
	return o.Apply(applier).(AssetOrArchiveMapOutput)
}

// ApplyAssetOrArchiveMapWithContext is like ApplyWithContext, but returns a AssetOrArchiveMapOutput.
func (o *OutputState) ApplyAssetOrArchiveMapWithContext(ctx context.Context, applier interface{}) AssetOrArchiveMapOutput {
	return o.ApplyWithContext(ctx, applier).(AssetOrArchiveMapOutput)
}

// ApplyBool is like Apply, but returns a BoolOutput.
func (o *OutputState) ApplyBool(applier interface{}) BoolOutput {
	return o.Apply(applier).(BoolOutput)
}

// ApplyBoolWithContext is like ApplyWithContext, but returns a BoolOutput.
func (o *OutputState) ApplyBoolWithContext(ctx context.Context, applier interface{}) BoolOutput {
	return o.ApplyWithContext(ctx, applier).(BoolOutput)
}

// ApplyBoolArray is like Apply, but returns a BoolArrayOutput.
func (o *OutputState) ApplyBoolArray(applier interface{}) BoolArrayOutput {
	return o.Apply(applier).(BoolArrayOutput)
}

// ApplyBoolArrayWithContext is like ApplyWithContext, but returns a BoolArrayOutput.
func (o *OutputState) ApplyBoolArrayWithContext(ctx context.Context, applier interface{}) BoolArrayOutput {
	return o.ApplyWithContext(ctx, applier).(BoolArrayOutput)
}

// ApplyBoolMap is like Apply, but returns a BoolMapOutput.
func (o *OutputState) ApplyBoolMap(applier interface{}) BoolMapOutput {
	return o.Apply(applier).(BoolMapOutput)
}

// ApplyBoolMapWithContext is like ApplyWithContext, but returns a BoolMapOutput.
func (o *OutputState) ApplyBoolMapWithContext(ctx context.Context, applier interface{}) BoolMapOutput {
	return o.ApplyWithContext(ctx, applier).(BoolMapOutput)
}

// ApplyFloat32 is like Apply, but returns a Float32Output.
func (o *OutputState) ApplyFloat32(applier interface{}) Float32Output {
	return o.Apply(applier).(Float32Output)
}

// ApplyFloat32WithContext is like ApplyWithContext, but returns a Float32Output.
func (o *OutputState) ApplyFloat32WithContext(ctx context.Context, applier interface{}) Float32Output {
	return o.ApplyWithContext(ctx, applier).(Float32Output)
}

// ApplyFloat32Array is like Apply, but returns a Float32ArrayOutput.
func (o *OutputState) ApplyFloat32Array(applier interface{}) Float32ArrayOutput {
	return o.Apply(applier).(Float32ArrayOutput)
}

// ApplyFloat32ArrayWithContext is like ApplyWithContext, but returns a Float32ArrayOutput.
func (o *OutputState) ApplyFloat32ArrayWithContext(ctx context.Context, applier interface{}) Float32ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Float32ArrayOutput)
}

// ApplyFloat32Map is like Apply, but returns a Float32MapOutput.
func (o *OutputState) ApplyFloat32Map(applier interface{}) Float32MapOutput {
	return o.Apply(applier).(Float32MapOutput)
}

// ApplyFloat32MapWithContext is like ApplyWithContext, but returns a Float32MapOutput.
func (o *OutputState) ApplyFloat32MapWithContext(ctx context.Context, applier interface{}) Float32MapOutput {
	return o.ApplyWithContext(ctx, applier).(Float32MapOutput)
}

// ApplyFloat64 is like Apply, but returns a Float64Output.
func (o *OutputState) ApplyFloat64(applier interface{}) Float64Output {
	return o.Apply(applier).(Float64Output)
}

// ApplyFloat64WithContext is like ApplyWithContext, but returns a Float64Output.
func (o *OutputState) ApplyFloat64WithContext(ctx context.Context, applier interface{}) Float64Output {
	return o.ApplyWithContext(ctx, applier).(Float64Output)
}

// ApplyFloat64Array is like Apply, but returns a Float64ArrayOutput.
func (o *OutputState) ApplyFloat64Array(applier interface{}) Float64ArrayOutput {
	return o.Apply(applier).(Float64ArrayOutput)
}

// ApplyFloat64ArrayWithContext is like ApplyWithContext, but returns a Float64ArrayOutput.
func (o *OutputState) ApplyFloat64ArrayWithContext(ctx context.Context, applier interface{}) Float64ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Float64ArrayOutput)
}

// ApplyFloat64Map is like Apply, but returns a Float64MapOutput.
func (o *OutputState) ApplyFloat64Map(applier interface{}) Float64MapOutput {
	return o.Apply(applier).(Float64MapOutput)
}

// ApplyFloat64MapWithContext is like ApplyWithContext, but returns a Float64MapOutput.
func (o *OutputState) ApplyFloat64MapWithContext(ctx context.Context, applier interface{}) Float64MapOutput {
	return o.ApplyWithContext(ctx, applier).(Float64MapOutput)
}

// ApplyID is like Apply, but returns a IDOutput.
func (o *OutputState) ApplyID(applier interface{}) IDOutput {
	return o.Apply(applier).(IDOutput)
}

// ApplyIDWithContext is like ApplyWithContext, but returns a IDOutput.
func (o *OutputState) ApplyIDWithContext(ctx context.Context, applier interface{}) IDOutput {
	return o.ApplyWithContext(ctx, applier).(IDOutput)
}

// ApplyIDArray is like Apply, but returns a IDArrayOutput.
func (o *OutputState) ApplyIDArray(applier interface{}) IDArrayOutput {
	return o.Apply(applier).(IDArrayOutput)
}

// ApplyIDArrayWithContext is like ApplyWithContext, but returns a IDArrayOutput.
func (o *OutputState) ApplyIDArrayWithContext(ctx context.Context, applier interface{}) IDArrayOutput {
	return o.ApplyWithContext(ctx, applier).(IDArrayOutput)
}

// ApplyIDMap is like Apply, but returns a IDMapOutput.
func (o *OutputState) ApplyIDMap(applier interface{}) IDMapOutput {
	return o.Apply(applier).(IDMapOutput)
}

// ApplyIDMapWithContext is like ApplyWithContext, but returns a IDMapOutput.
func (o *OutputState) ApplyIDMapWithContext(ctx context.Context, applier interface{}) IDMapOutput {
	return o.ApplyWithContext(ctx, applier).(IDMapOutput)
}

// ApplyInt is like Apply, but returns a IntOutput.
func (o *OutputState) ApplyInt(applier interface{}) IntOutput {
	return o.Apply(applier).(IntOutput)
}

// ApplyIntWithContext is like ApplyWithContext, but returns a IntOutput.
func (o *OutputState) ApplyIntWithContext(ctx context.Context, applier interface{}) IntOutput {
	return o.ApplyWithContext(ctx, applier).(IntOutput)
}

// ApplyIntArray is like Apply, but returns a IntArrayOutput.
func (o *OutputState) ApplyIntArray(applier interface{}) IntArrayOutput {
	return o.Apply(applier).(IntArrayOutput)
}

// ApplyIntArrayWithContext is like ApplyWithContext, but returns a IntArrayOutput.
func (o *OutputState) ApplyIntArrayWithContext(ctx context.Context, applier interface{}) IntArrayOutput {
	return o.ApplyWithContext(ctx, applier).(IntArrayOutput)
}

// ApplyIntMap is like Apply, but returns a IntMapOutput.
func (o *OutputState) ApplyIntMap(applier interface{}) IntMapOutput {
	return o.Apply(applier).(IntMapOutput)
}

// ApplyIntMapWithContext is like ApplyWithContext, but returns a IntMapOutput.
func (o *OutputState) ApplyIntMapWithContext(ctx context.Context, applier interface{}) IntMapOutput {
	return o.ApplyWithContext(ctx, applier).(IntMapOutput)
}

// ApplyInt16 is like Apply, but returns a Int16Output.
func (o *OutputState) ApplyInt16(applier interface{}) Int16Output {
	return o.Apply(applier).(Int16Output)
}

// ApplyInt16WithContext is like ApplyWithContext, but returns a Int16Output.
func (o *OutputState) ApplyInt16WithContext(ctx context.Context, applier interface{}) Int16Output {
	return o.ApplyWithContext(ctx, applier).(Int16Output)
}

// ApplyInt16Array is like Apply, but returns a Int16ArrayOutput.
func (o *OutputState) ApplyInt16Array(applier interface{}) Int16ArrayOutput {
	return o.Apply(applier).(Int16ArrayOutput)
}

// ApplyInt16ArrayWithContext is like ApplyWithContext, but returns a Int16ArrayOutput.
func (o *OutputState) ApplyInt16ArrayWithContext(ctx context.Context, applier interface{}) Int16ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Int16ArrayOutput)
}

// ApplyInt16Map is like Apply, but returns a Int16MapOutput.
func (o *OutputState) ApplyInt16Map(applier interface{}) Int16MapOutput {
	return o.Apply(applier).(Int16MapOutput)
}

// ApplyInt16MapWithContext is like ApplyWithContext, but returns a Int16MapOutput.
func (o *OutputState) ApplyInt16MapWithContext(ctx context.Context, applier interface{}) Int16MapOutput {
	return o.ApplyWithContext(ctx, applier).(Int16MapOutput)
}

// ApplyInt32 is like Apply, but returns a Int32Output.
func (o *OutputState) ApplyInt32(applier interface{}) Int32Output {
	return o.Apply(applier).(Int32Output)
}

// ApplyInt32WithContext is like ApplyWithContext, but returns a Int32Output.
func (o *OutputState) ApplyInt32WithContext(ctx context.Context, applier interface{}) Int32Output {
	return o.ApplyWithContext(ctx, applier).(Int32Output)
}

// ApplyInt32Array is like Apply, but returns a Int32ArrayOutput.
func (o *OutputState) ApplyInt32Array(applier interface{}) Int32ArrayOutput {
	return o.Apply(applier).(Int32ArrayOutput)
}

// ApplyInt32ArrayWithContext is like ApplyWithContext, but returns a Int32ArrayOutput.
func (o *OutputState) ApplyInt32ArrayWithContext(ctx context.Context, applier interface{}) Int32ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Int32ArrayOutput)
}

// ApplyInt32Map is like Apply, but returns a Int32MapOutput.
func (o *OutputState) ApplyInt32Map(applier interface{}) Int32MapOutput {
	return o.Apply(applier).(Int32MapOutput)
}

// ApplyInt32MapWithContext is like ApplyWithContext, but returns a Int32MapOutput.
func (o *OutputState) ApplyInt32MapWithContext(ctx context.Context, applier interface{}) Int32MapOutput {
	return o.ApplyWithContext(ctx, applier).(Int32MapOutput)
}

// ApplyInt64 is like Apply, but returns a Int64Output.
func (o *OutputState) ApplyInt64(applier interface{}) Int64Output {
	return o.Apply(applier).(Int64Output)
}

// ApplyInt64WithContext is like ApplyWithContext, but returns a Int64Output.
func (o *OutputState) ApplyInt64WithContext(ctx context.Context, applier interface{}) Int64Output {
	return o.ApplyWithContext(ctx, applier).(Int64Output)
}

// ApplyInt64Array is like Apply, but returns a Int64ArrayOutput.
func (o *OutputState) ApplyInt64Array(applier interface{}) Int64ArrayOutput {
	return o.Apply(applier).(Int64ArrayOutput)
}

// ApplyInt64ArrayWithContext is like ApplyWithContext, but returns a Int64ArrayOutput.
func (o *OutputState) ApplyInt64ArrayWithContext(ctx context.Context, applier interface{}) Int64ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Int64ArrayOutput)
}

// ApplyInt64Map is like Apply, but returns a Int64MapOutput.
func (o *OutputState) ApplyInt64Map(applier interface{}) Int64MapOutput {
	return o.Apply(applier).(Int64MapOutput)
}

// ApplyInt64MapWithContext is like ApplyWithContext, but returns a Int64MapOutput.
func (o *OutputState) ApplyInt64MapWithContext(ctx context.Context, applier interface{}) Int64MapOutput {
	return o.ApplyWithContext(ctx, applier).(Int64MapOutput)
}

// ApplyInt8 is like Apply, but returns a Int8Output.
func (o *OutputState) ApplyInt8(applier interface{}) Int8Output {
	return o.Apply(applier).(Int8Output)
}

// ApplyInt8WithContext is like ApplyWithContext, but returns a Int8Output.
func (o *OutputState) ApplyInt8WithContext(ctx context.Context, applier interface{}) Int8Output {
	return o.ApplyWithContext(ctx, applier).(Int8Output)
}

// ApplyInt8Array is like Apply, but returns a Int8ArrayOutput.
func (o *OutputState) ApplyInt8Array(applier interface{}) Int8ArrayOutput {
	return o.Apply(applier).(Int8ArrayOutput)
}

// ApplyInt8ArrayWithContext is like ApplyWithContext, but returns a Int8ArrayOutput.
func (o *OutputState) ApplyInt8ArrayWithContext(ctx context.Context, applier interface{}) Int8ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Int8ArrayOutput)
}

// ApplyInt8Map is like Apply, but returns a Int8MapOutput.
func (o *OutputState) ApplyInt8Map(applier interface{}) Int8MapOutput {
	return o.Apply(applier).(Int8MapOutput)
}

// ApplyInt8MapWithContext is like ApplyWithContext, but returns a Int8MapOutput.
func (o *OutputState) ApplyInt8MapWithContext(ctx context.Context, applier interface{}) Int8MapOutput {
	return o.ApplyWithContext(ctx, applier).(Int8MapOutput)
}

// ApplyString is like Apply, but returns a StringOutput.
func (o *OutputState) ApplyString(applier interface{}) StringOutput {
	return o.Apply(applier).(StringOutput)
}

// ApplyStringWithContext is like ApplyWithContext, but returns a StringOutput.
func (o *OutputState) ApplyStringWithContext(ctx context.Context, applier interface{}) StringOutput {
	return o.ApplyWithContext(ctx, applier).(StringOutput)
}

// ApplyStringArray is like Apply, but returns a StringArrayOutput.
func (o *OutputState) ApplyStringArray(applier interface{}) StringArrayOutput {
	return o.Apply(applier).(StringArrayOutput)
}

// ApplyStringArrayWithContext is like ApplyWithContext, but returns a StringArrayOutput.
func (o *OutputState) ApplyStringArrayWithContext(ctx context.Context, applier interface{}) StringArrayOutput {
	return o.ApplyWithContext(ctx, applier).(StringArrayOutput)
}

// ApplyStringMap is like Apply, but returns a StringMapOutput.
func (o *OutputState) ApplyStringMap(applier interface{}) StringMapOutput {
	return o.Apply(applier).(StringMapOutput)
}

// ApplyStringMapWithContext is like ApplyWithContext, but returns a StringMapOutput.
func (o *OutputState) ApplyStringMapWithContext(ctx context.Context, applier interface{}) StringMapOutput {
	return o.ApplyWithContext(ctx, applier).(StringMapOutput)
}

// ApplyURN is like Apply, but returns a URNOutput.
func (o *OutputState) ApplyURN(applier interface{}) URNOutput {
	return o.Apply(applier).(URNOutput)
}

// ApplyURNWithContext is like ApplyWithContext, but returns a URNOutput.
func (o *OutputState) ApplyURNWithContext(ctx context.Context, applier interface{}) URNOutput {
	return o.ApplyWithContext(ctx, applier).(URNOutput)
}

// ApplyURNArray is like Apply, but returns a URNArrayOutput.
func (o *OutputState) ApplyURNArray(applier interface{}) URNArrayOutput {
	return o.Apply(applier).(URNArrayOutput)
}

// ApplyURNArrayWithContext is like ApplyWithContext, but returns a URNArrayOutput.
func (o *OutputState) ApplyURNArrayWithContext(ctx context.Context, applier interface{}) URNArrayOutput {
	return o.ApplyWithContext(ctx, applier).(URNArrayOutput)
}

// ApplyURNMap is like Apply, but returns a URNMapOutput.
func (o *OutputState) ApplyURNMap(applier interface{}) URNMapOutput {
	return o.Apply(applier).(URNMapOutput)
}

// ApplyURNMapWithContext is like ApplyWithContext, but returns a URNMapOutput.
func (o *OutputState) ApplyURNMapWithContext(ctx context.Context, applier interface{}) URNMapOutput {
	return o.ApplyWithContext(ctx, applier).(URNMapOutput)
}

// ApplyUint is like Apply, but returns a UintOutput.
func (o *OutputState) ApplyUint(applier interface{}) UintOutput {
	return o.Apply(applier).(UintOutput)
}

// ApplyUintWithContext is like ApplyWithContext, but returns a UintOutput.
func (o *OutputState) ApplyUintWithContext(ctx context.Context, applier interface{}) UintOutput {
	return o.ApplyWithContext(ctx, applier).(UintOutput)
}

// ApplyUintArray is like Apply, but returns a UintArrayOutput.
func (o *OutputState) ApplyUintArray(applier interface{}) UintArrayOutput {
	return o.Apply(applier).(UintArrayOutput)
}

// ApplyUintArrayWithContext is like ApplyWithContext, but returns a UintArrayOutput.
func (o *OutputState) ApplyUintArrayWithContext(ctx context.Context, applier interface{}) UintArrayOutput {
	return o.ApplyWithContext(ctx, applier).(UintArrayOutput)
}

// ApplyUintMap is like Apply, but returns a UintMapOutput.
func (o *OutputState) ApplyUintMap(applier interface{}) UintMapOutput {
	return o.Apply(applier).(UintMapOutput)
}

// ApplyUintMapWithContext is like ApplyWithContext, but returns a UintMapOutput.
func (o *OutputState) ApplyUintMapWithContext(ctx context.Context, applier interface{}) UintMapOutput {
	return o.ApplyWithContext(ctx, applier).(UintMapOutput)
}

// ApplyUint16 is like Apply, but returns a Uint16Output.
func (o *OutputState) ApplyUint16(applier interface{}) Uint16Output {
	return o.Apply(applier).(Uint16Output)
}

// ApplyUint16WithContext is like ApplyWithContext, but returns a Uint16Output.
func (o *OutputState) ApplyUint16WithContext(ctx context.Context, applier interface{}) Uint16Output {
	return o.ApplyWithContext(ctx, applier).(Uint16Output)
}

// ApplyUint16Array is like Apply, but returns a Uint16ArrayOutput.
func (o *OutputState) ApplyUint16Array(applier interface{}) Uint16ArrayOutput {
	return o.Apply(applier).(Uint16ArrayOutput)
}

// ApplyUint16ArrayWithContext is like ApplyWithContext, but returns a Uint16ArrayOutput.
func (o *OutputState) ApplyUint16ArrayWithContext(ctx context.Context, applier interface{}) Uint16ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Uint16ArrayOutput)
}

// ApplyUint16Map is like Apply, but returns a Uint16MapOutput.
func (o *OutputState) ApplyUint16Map(applier interface{}) Uint16MapOutput {
	return o.Apply(applier).(Uint16MapOutput)
}

// ApplyUint16MapWithContext is like ApplyWithContext, but returns a Uint16MapOutput.
func (o *OutputState) ApplyUint16MapWithContext(ctx context.Context, applier interface{}) Uint16MapOutput {
	return o.ApplyWithContext(ctx, applier).(Uint16MapOutput)
}

// ApplyUint32 is like Apply, but returns a Uint32Output.
func (o *OutputState) ApplyUint32(applier interface{}) Uint32Output {
	return o.Apply(applier).(Uint32Output)
}

// ApplyUint32WithContext is like ApplyWithContext, but returns a Uint32Output.
func (o *OutputState) ApplyUint32WithContext(ctx context.Context, applier interface{}) Uint32Output {
	return o.ApplyWithContext(ctx, applier).(Uint32Output)
}

// ApplyUint32Array is like Apply, but returns a Uint32ArrayOutput.
func (o *OutputState) ApplyUint32Array(applier interface{}) Uint32ArrayOutput {
	return o.Apply(applier).(Uint32ArrayOutput)
}

// ApplyUint32ArrayWithContext is like ApplyWithContext, but returns a Uint32ArrayOutput.
func (o *OutputState) ApplyUint32ArrayWithContext(ctx context.Context, applier interface{}) Uint32ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Uint32ArrayOutput)
}

// ApplyUint32Map is like Apply, but returns a Uint32MapOutput.
func (o *OutputState) ApplyUint32Map(applier interface{}) Uint32MapOutput {
	return o.Apply(applier).(Uint32MapOutput)
}

// ApplyUint32MapWithContext is like ApplyWithContext, but returns a Uint32MapOutput.
func (o *OutputState) ApplyUint32MapWithContext(ctx context.Context, applier interface{}) Uint32MapOutput {
	return o.ApplyWithContext(ctx, applier).(Uint32MapOutput)
}

// ApplyUint64 is like Apply, but returns a Uint64Output.
func (o *OutputState) ApplyUint64(applier interface{}) Uint64Output {
	return o.Apply(applier).(Uint64Output)
}

// ApplyUint64WithContext is like ApplyWithContext, but returns a Uint64Output.
func (o *OutputState) ApplyUint64WithContext(ctx context.Context, applier interface{}) Uint64Output {
	return o.ApplyWithContext(ctx, applier).(Uint64Output)
}

// ApplyUint64Array is like Apply, but returns a Uint64ArrayOutput.
func (o *OutputState) ApplyUint64Array(applier interface{}) Uint64ArrayOutput {
	return o.Apply(applier).(Uint64ArrayOutput)
}

// ApplyUint64ArrayWithContext is like ApplyWithContext, but returns a Uint64ArrayOutput.
func (o *OutputState) ApplyUint64ArrayWithContext(ctx context.Context, applier interface{}) Uint64ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Uint64ArrayOutput)
}

// ApplyUint64Map is like Apply, but returns a Uint64MapOutput.
func (o *OutputState) ApplyUint64Map(applier interface{}) Uint64MapOutput {
	return o.Apply(applier).(Uint64MapOutput)
}

// ApplyUint64MapWithContext is like ApplyWithContext, but returns a Uint64MapOutput.
func (o *OutputState) ApplyUint64MapWithContext(ctx context.Context, applier interface{}) Uint64MapOutput {
	return o.ApplyWithContext(ctx, applier).(Uint64MapOutput)
}

// ApplyUint8 is like Apply, but returns a Uint8Output.
func (o *OutputState) ApplyUint8(applier interface{}) Uint8Output {
	return o.Apply(applier).(Uint8Output)
}

// ApplyUint8WithContext is like ApplyWithContext, but returns a Uint8Output.
func (o *OutputState) ApplyUint8WithContext(ctx context.Context, applier interface{}) Uint8Output {
	return o.ApplyWithContext(ctx, applier).(Uint8Output)
}

// ApplyUint8Array is like Apply, but returns a Uint8ArrayOutput.
func (o *OutputState) ApplyUint8Array(applier interface{}) Uint8ArrayOutput {
	return o.Apply(applier).(Uint8ArrayOutput)
}

// ApplyUint8ArrayWithContext is like ApplyWithContext, but returns a Uint8ArrayOutput.
func (o *OutputState) ApplyUint8ArrayWithContext(ctx context.Context, applier interface{}) Uint8ArrayOutput {
	return o.ApplyWithContext(ctx, applier).(Uint8ArrayOutput)
}

// ApplyUint8Map is like Apply, but returns a Uint8MapOutput.
func (o *OutputState) ApplyUint8Map(applier interface{}) Uint8MapOutput {
	return o.Apply(applier).(Uint8MapOutput)
}

// ApplyUint8MapWithContext is like ApplyWithContext, but returns a Uint8MapOutput.
func (o *OutputState) ApplyUint8MapWithContext(ctx context.Context, applier interface{}) Uint8MapOutput {
	return o.ApplyWithContext(ctx, applier).(Uint8MapOutput)
}

// All returns an AnyArrayOutput that will resolve when all of the provided outputs will resolve. Each element of the
// array will contain the resolved value of the corresponding output. The output will be rejected if any of the inputs
// is rejected.
func All(outputs ...Output) AnyArrayOutput {
	return AllWithContext(context.Background(), outputs...)
}

// AllWithContext returns an AnyArrayOutput that will resolve when all of the provided outputs will resolve. Each
// element of the array will contain the resolved value of the corresponding output. The output will be rejected if any
// of the inputs is rejected.
func AllWithContext(ctx context.Context, outputs ...Output) AnyArrayOutput {
	var deps []Resource
	for _, o := range outputs {
		deps = append(deps, o.dependencies()...)
	}

	result := newOutputState(anyArrayType, deps...)
	go func() {
		arr := make([]interface{}, len(outputs))

		known := true
		for i, o := range outputs {
			ov, oKnown, err := o.await(ctx)
			if err != nil {
				result.reject(err)
			}
			arr[i], known = ov, known && oKnown
		}
		result.fulfill(arr, known, nil)
	}()
	return AnyArrayOutput{result}
}

// Input is the type of a generic input value for a Pulumi resource. This type is used in conjunction with Output
// to provide polymorphism over strongly-typed input values.
//
// The intended pattern for nested Pulumi value types is to define an input interface and a plain, input, and output
// variant of the value type that implement the input interface.
//
// For example, given a nested Pulumi value type with the following shape:
//
//     type Nested struct {
//         Foo int
//         Bar string
//     }
//
// We would define the following:
//
//     var nestedType = reflect.TypeOf((*Nested)(nil))
//
//     type NestedInputType interface {
//         pulumi.Input
//
//         isNested()
//     }
//
//     type Nested struct {
//         Foo int `pulumi:"foo"`
//         Bar string `pulumi:"bar"`
//     }
//
//     func (*Nested) ElementType() reflect.Type {
//         return nestedType
//     }
//
//     func (*Nested) isNested() {}
//
//     type NestedInput struct {
//         Foo pulumi.IntInput `pulumi:"foo"`
//         Bar pulumi.StringInput `pulumi:"bar"`
//     }
//
//     func (*NestedInput) ElementType() reflect.Type {
//         return nestedType
//     }
//
//     func (*NestedInput) isNested() {}
//
//     type NestedOutput pulumi.Output
//
//     func (NestedOutput) ElementType() reflect.Type {
//         return nestedType
//     }
//
//     func (NestedOutput) isNested() {}
//
//     func (out NestedOutput) Apply(applier func(*Nested) (interface{}, error)) {
//         return out.ApplyWithContext(context.Background(), func(_ context.Context, v *Nested) (interface{}, error) {
//             return applier(v)
//         })
//     }
//
//     func (out NestedOutput) ApplyWithContext(ctx context.Context, applier func(context.Context, *Nested) (interface{}, error) {
//         return pulumi.Output(out).ApplyWithContext(ctx, func(ctx context.Context, v interface{}) (interface{}, error) {
//             return applier(ctx, v.(*Nested))
//         })
//     }
//
type Input interface {
	ElementType() reflect.Type
}

type anyInput struct {
	v interface{}
}

// Any creates a new AnyInput value.
func Any(v interface{}) AnyInput {
	return anyInput{v: v}
}

var anyType = reflect.TypeOf((*interface{})(nil)).Elem()

// AnyInput is an input type that accepts Any and AnyOutput values.
type AnyInput interface {
	Input

	// nolint: unused
	isAny()
}

// ElementType returns the element type of this Input (interface{}).
func (anyInput) ElementType() reflect.Type {
	return anyType
}

func (anyInput) isAny() {}

// AnyOutput is an Output that returns interface{} values.
type AnyOutput struct{ *OutputState }

// ElementType returns the element type of this Output (interface{}).
func (AnyOutput) ElementType() reflect.Type {
	return anyType
}

func (AnyOutput) isAny() {}

var anyArrayType = reflect.TypeOf((*[]interface{})(nil)).Elem()

// AnyArrayInput is an input type that accepts AnyArray and AnyArrayOutput values.
type AnyArrayInput interface {
	Input

	// nolint: unused
	isAnyArray()
}

// AnyArray is an input type for []AnyInput values.
type AnyArray []AnyInput

// ElementType returns the element type of this Input ([]interface{}).
func (AnyArray) ElementType() reflect.Type {
	return anyArrayType
}

func (AnyArray) isAnyArray() {}

// AnyArrayOutput is an Output that returns []AnyInput values.
type AnyArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]interface{}).
func (AnyArrayOutput) ElementType() reflect.Type {
	return anyArrayType
}

func (AnyArrayOutput) isAnyArray() {}

var anyMapType = reflect.TypeOf((*map[string]interface{})(nil)).Elem()

// AnyMapInput is an input type that accepts AnyMap and AnyMapOutput values.
type AnyMapInput interface {
	Input

	// nolint: unused
	isAnyMap()
}

// AnyMap is an input type for map[string]AnyInput values.
type AnyMap map[string]AnyInput

// ElementType returns the element type of this Input (map[string]interface{}).
func (AnyMap) ElementType() reflect.Type {
	return anyMapType
}

func (AnyMap) isAnyMap() {}

// AnyMapOutput is an Output that returns map[string]AnyInput values.
type AnyMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]interface{}).
func (AnyMapOutput) ElementType() reflect.Type {
	return anyMapType
}

func (AnyMapOutput) isAnyMap() {}

var archiveType = reflect.TypeOf((*Archive)(nil)).Elem()

// ArchiveInput is an input type that accepts Archive and ArchiveOutput values.
type ArchiveInput interface {
	Input

	// nolint: unused
	isArchive()
}

// ElementType returns the element type of this Input (Archive).
func (*archive) ElementType() reflect.Type {
	return archiveType
}

func (*archive) isArchive() {}

func (*archive) isAssetOrArchive() {}

// ArchiveOutput is an Output that returns Archive values.
type ArchiveOutput struct{ *OutputState }

// ElementType returns the element type of this Output (Archive).
func (ArchiveOutput) ElementType() reflect.Type {
	return archiveType
}

func (ArchiveOutput) isArchive() {}

func (ArchiveOutput) isAssetOrArchive() {}

var archiveArrayType = reflect.TypeOf((*[]Archive)(nil)).Elem()

// ArchiveArrayInput is an input type that accepts ArchiveArray and ArchiveArrayOutput values.
type ArchiveArrayInput interface {
	Input

	// nolint: unused
	isArchiveArray()
}

// ArchiveArray is an input type for []ArchiveInput values.
type ArchiveArray []ArchiveInput

// ElementType returns the element type of this Input ([]Archive).
func (ArchiveArray) ElementType() reflect.Type {
	return archiveArrayType
}

func (ArchiveArray) isArchiveArray() {}

// ArchiveArrayOutput is an Output that returns []ArchiveInput values.
type ArchiveArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]Archive).
func (ArchiveArrayOutput) ElementType() reflect.Type {
	return archiveArrayType
}

func (ArchiveArrayOutput) isArchiveArray() {}

var archiveMapType = reflect.TypeOf((*map[string]Archive)(nil)).Elem()

// ArchiveMapInput is an input type that accepts ArchiveMap and ArchiveMapOutput values.
type ArchiveMapInput interface {
	Input

	// nolint: unused
	isArchiveMap()
}

// ArchiveMap is an input type for map[string]ArchiveInput values.
type ArchiveMap map[string]ArchiveInput

// ElementType returns the element type of this Input (map[string]Archive).
func (ArchiveMap) ElementType() reflect.Type {
	return archiveMapType
}

func (ArchiveMap) isArchiveMap() {}

// ArchiveMapOutput is an Output that returns map[string]ArchiveInput values.
type ArchiveMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]Archive).
func (ArchiveMapOutput) ElementType() reflect.Type {
	return archiveMapType
}

func (ArchiveMapOutput) isArchiveMap() {}

var assetType = reflect.TypeOf((*Asset)(nil)).Elem()

// AssetInput is an input type that accepts Asset and AssetOutput values.
type AssetInput interface {
	Input

	// nolint: unused
	isAsset()
}

// ElementType returns the element type of this Input (Asset).
func (*asset) ElementType() reflect.Type {
	return assetType
}

func (*asset) isAsset() {}

func (*asset) isAssetOrArchive() {}

// AssetOutput is an Output that returns Asset values.
type AssetOutput struct{ *OutputState }

// ElementType returns the element type of this Output (Asset).
func (AssetOutput) ElementType() reflect.Type {
	return assetType
}

func (AssetOutput) isAsset() {}

func (AssetOutput) isAssetOrArchive() {}

var assetArrayType = reflect.TypeOf((*[]Asset)(nil)).Elem()

// AssetArrayInput is an input type that accepts AssetArray and AssetArrayOutput values.
type AssetArrayInput interface {
	Input

	// nolint: unused
	isAssetArray()
}

// AssetArray is an input type for []AssetInput values.
type AssetArray []AssetInput

// ElementType returns the element type of this Input ([]Asset).
func (AssetArray) ElementType() reflect.Type {
	return assetArrayType
}

func (AssetArray) isAssetArray() {}

// AssetArrayOutput is an Output that returns []AssetInput values.
type AssetArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]Asset).
func (AssetArrayOutput) ElementType() reflect.Type {
	return assetArrayType
}

func (AssetArrayOutput) isAssetArray() {}

var assetMapType = reflect.TypeOf((*map[string]Asset)(nil)).Elem()

// AssetMapInput is an input type that accepts AssetMap and AssetMapOutput values.
type AssetMapInput interface {
	Input

	// nolint: unused
	isAssetMap()
}

// AssetMap is an input type for map[string]AssetInput values.
type AssetMap map[string]AssetInput

// ElementType returns the element type of this Input (map[string]Asset).
func (AssetMap) ElementType() reflect.Type {
	return assetMapType
}

func (AssetMap) isAssetMap() {}

// AssetMapOutput is an Output that returns map[string]AssetInput values.
type AssetMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]Asset).
func (AssetMapOutput) ElementType() reflect.Type {
	return assetMapType
}

func (AssetMapOutput) isAssetMap() {}

var assetOrArchiveType = reflect.TypeOf((*AssetOrArchive)(nil)).Elem()

// AssetOrArchiveInput is an input type that accepts AssetOrArchive and AssetOrArchiveOutput values.
type AssetOrArchiveInput interface {
	Input

	// nolint: unused
	isAssetOrArchive()
}

// AssetOrArchiveOutput is an Output that returns AssetOrArchive values.
type AssetOrArchiveOutput struct{ *OutputState }

// ElementType returns the element type of this Output (AssetOrArchive).
func (AssetOrArchiveOutput) ElementType() reflect.Type {
	return assetOrArchiveType
}

func (AssetOrArchiveOutput) isAssetOrArchive() {}

var assetOrArchiveArrayType = reflect.TypeOf((*[]AssetOrArchive)(nil)).Elem()

// AssetOrArchiveArrayInput is an input type that accepts AssetOrArchiveArray and AssetOrArchiveArrayOutput values.
type AssetOrArchiveArrayInput interface {
	Input

	// nolint: unused
	isAssetOrArchiveArray()
}

// AssetOrArchiveArray is an input type for []AssetOrArchiveInput values.
type AssetOrArchiveArray []AssetOrArchiveInput

// ElementType returns the element type of this Input ([]AssetOrArchive).
func (AssetOrArchiveArray) ElementType() reflect.Type {
	return assetOrArchiveArrayType
}

func (AssetOrArchiveArray) isAssetOrArchiveArray() {}

// AssetOrArchiveArrayOutput is an Output that returns []AssetOrArchiveInput values.
type AssetOrArchiveArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]AssetOrArchive).
func (AssetOrArchiveArrayOutput) ElementType() reflect.Type {
	return assetOrArchiveArrayType
}

func (AssetOrArchiveArrayOutput) isAssetOrArchiveArray() {}

var assetOrArchiveMapType = reflect.TypeOf((*map[string]AssetOrArchive)(nil)).Elem()

// AssetOrArchiveMapInput is an input type that accepts AssetOrArchiveMap and AssetOrArchiveMapOutput values.
type AssetOrArchiveMapInput interface {
	Input

	// nolint: unused
	isAssetOrArchiveMap()
}

// AssetOrArchiveMap is an input type for map[string]AssetOrArchiveInput values.
type AssetOrArchiveMap map[string]AssetOrArchiveInput

// ElementType returns the element type of this Input (map[string]AssetOrArchive).
func (AssetOrArchiveMap) ElementType() reflect.Type {
	return assetOrArchiveMapType
}

func (AssetOrArchiveMap) isAssetOrArchiveMap() {}

// AssetOrArchiveMapOutput is an Output that returns map[string]AssetOrArchiveInput values.
type AssetOrArchiveMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]AssetOrArchive).
func (AssetOrArchiveMapOutput) ElementType() reflect.Type {
	return assetOrArchiveMapType
}

func (AssetOrArchiveMapOutput) isAssetOrArchiveMap() {}

var boolType = reflect.TypeOf((*bool)(nil)).Elem()

// BoolInput is an input type that accepts Bool and BoolOutput values.
type BoolInput interface {
	Input

	// nolint: unused
	isBool()
}

// Bool is an input type for bool values.
type Bool bool

// ElementType returns the element type of this Input (bool).
func (Bool) ElementType() reflect.Type {
	return boolType
}

func (Bool) isBool() {}

// BoolOutput is an Output that returns bool values.
type BoolOutput struct{ *OutputState }

// ElementType returns the element type of this Output (bool).
func (BoolOutput) ElementType() reflect.Type {
	return boolType
}

func (BoolOutput) isBool() {}

var boolArrayType = reflect.TypeOf((*[]bool)(nil)).Elem()

// BoolArrayInput is an input type that accepts BoolArray and BoolArrayOutput values.
type BoolArrayInput interface {
	Input

	// nolint: unused
	isBoolArray()
}

// BoolArray is an input type for []BoolInput values.
type BoolArray []BoolInput

// ElementType returns the element type of this Input ([]bool).
func (BoolArray) ElementType() reflect.Type {
	return boolArrayType
}

func (BoolArray) isBoolArray() {}

// BoolArrayOutput is an Output that returns []BoolInput values.
type BoolArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]bool).
func (BoolArrayOutput) ElementType() reflect.Type {
	return boolArrayType
}

func (BoolArrayOutput) isBoolArray() {}

var boolMapType = reflect.TypeOf((*map[string]bool)(nil)).Elem()

// BoolMapInput is an input type that accepts BoolMap and BoolMapOutput values.
type BoolMapInput interface {
	Input

	// nolint: unused
	isBoolMap()
}

// BoolMap is an input type for map[string]BoolInput values.
type BoolMap map[string]BoolInput

// ElementType returns the element type of this Input (map[string]bool).
func (BoolMap) ElementType() reflect.Type {
	return boolMapType
}

func (BoolMap) isBoolMap() {}

// BoolMapOutput is an Output that returns map[string]BoolInput values.
type BoolMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]bool).
func (BoolMapOutput) ElementType() reflect.Type {
	return boolMapType
}

func (BoolMapOutput) isBoolMap() {}

var float32Type = reflect.TypeOf((*float32)(nil)).Elem()

// Float32Input is an input type that accepts Float32 and Float32Output values.
type Float32Input interface {
	Input

	// nolint: unused
	isFloat32()
}

// Float32 is an input type for float32 values.
type Float32 float32

// ElementType returns the element type of this Input (float32).
func (Float32) ElementType() reflect.Type {
	return float32Type
}

func (Float32) isFloat32() {}

// Float32Output is an Output that returns float32 values.
type Float32Output struct{ *OutputState }

// ElementType returns the element type of this Output (float32).
func (Float32Output) ElementType() reflect.Type {
	return float32Type
}

func (Float32Output) isFloat32() {}

var float32ArrayType = reflect.TypeOf((*[]float32)(nil)).Elem()

// Float32ArrayInput is an input type that accepts Float32Array and Float32ArrayOutput values.
type Float32ArrayInput interface {
	Input

	// nolint: unused
	isFloat32Array()
}

// Float32Array is an input type for []Float32Input values.
type Float32Array []Float32Input

// ElementType returns the element type of this Input ([]float32).
func (Float32Array) ElementType() reflect.Type {
	return float32ArrayType
}

func (Float32Array) isFloat32Array() {}

// Float32ArrayOutput is an Output that returns []Float32Input values.
type Float32ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]float32).
func (Float32ArrayOutput) ElementType() reflect.Type {
	return float32ArrayType
}

func (Float32ArrayOutput) isFloat32Array() {}

var float32MapType = reflect.TypeOf((*map[string]float32)(nil)).Elem()

// Float32MapInput is an input type that accepts Float32Map and Float32MapOutput values.
type Float32MapInput interface {
	Input

	// nolint: unused
	isFloat32Map()
}

// Float32Map is an input type for map[string]Float32Input values.
type Float32Map map[string]Float32Input

// ElementType returns the element type of this Input (map[string]float32).
func (Float32Map) ElementType() reflect.Type {
	return float32MapType
}

func (Float32Map) isFloat32Map() {}

// Float32MapOutput is an Output that returns map[string]Float32Input values.
type Float32MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]float32).
func (Float32MapOutput) ElementType() reflect.Type {
	return float32MapType
}

func (Float32MapOutput) isFloat32Map() {}

var float64Type = reflect.TypeOf((*float64)(nil)).Elem()

// Float64Input is an input type that accepts Float64 and Float64Output values.
type Float64Input interface {
	Input

	// nolint: unused
	isFloat64()
}

// Float64 is an input type for float64 values.
type Float64 float64

// ElementType returns the element type of this Input (float64).
func (Float64) ElementType() reflect.Type {
	return float64Type
}

func (Float64) isFloat64() {}

// Float64Output is an Output that returns float64 values.
type Float64Output struct{ *OutputState }

// ElementType returns the element type of this Output (float64).
func (Float64Output) ElementType() reflect.Type {
	return float64Type
}

func (Float64Output) isFloat64() {}

var float64ArrayType = reflect.TypeOf((*[]float64)(nil)).Elem()

// Float64ArrayInput is an input type that accepts Float64Array and Float64ArrayOutput values.
type Float64ArrayInput interface {
	Input

	// nolint: unused
	isFloat64Array()
}

// Float64Array is an input type for []Float64Input values.
type Float64Array []Float64Input

// ElementType returns the element type of this Input ([]float64).
func (Float64Array) ElementType() reflect.Type {
	return float64ArrayType
}

func (Float64Array) isFloat64Array() {}

// Float64ArrayOutput is an Output that returns []Float64Input values.
type Float64ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]float64).
func (Float64ArrayOutput) ElementType() reflect.Type {
	return float64ArrayType
}

func (Float64ArrayOutput) isFloat64Array() {}

var float64MapType = reflect.TypeOf((*map[string]float64)(nil)).Elem()

// Float64MapInput is an input type that accepts Float64Map and Float64MapOutput values.
type Float64MapInput interface {
	Input

	// nolint: unused
	isFloat64Map()
}

// Float64Map is an input type for map[string]Float64Input values.
type Float64Map map[string]Float64Input

// ElementType returns the element type of this Input (map[string]float64).
func (Float64Map) ElementType() reflect.Type {
	return float64MapType
}

func (Float64Map) isFloat64Map() {}

// Float64MapOutput is an Output that returns map[string]Float64Input values.
type Float64MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]float64).
func (Float64MapOutput) ElementType() reflect.Type {
	return float64MapType
}

func (Float64MapOutput) isFloat64Map() {}

var iDType = reflect.TypeOf((*ID)(nil)).Elem()

// IDInput is an input type that accepts ID and IDOutput values.
type IDInput interface {
	Input

	// nolint: unused
	isID()
}

// ElementType returns the element type of this Input (ID).
func (ID) ElementType() reflect.Type {
	return iDType
}

func (ID) isID() {}

func (ID) isString() {}

// IDOutput is an Output that returns ID values.
type IDOutput struct{ *OutputState }

// ElementType returns the element type of this Output (ID).
func (IDOutput) ElementType() reflect.Type {
	return iDType
}

func (IDOutput) isID() {}

func (IDOutput) isString() {}

var iDArrayType = reflect.TypeOf((*[]ID)(nil)).Elem()

// IDArrayInput is an input type that accepts IDArray and IDArrayOutput values.
type IDArrayInput interface {
	Input

	// nolint: unused
	isIDArray()
}

// IDArray is an input type for []IDInput values.
type IDArray []IDInput

// ElementType returns the element type of this Input ([]ID).
func (IDArray) ElementType() reflect.Type {
	return iDArrayType
}

func (IDArray) isIDArray() {}

// IDArrayOutput is an Output that returns []IDInput values.
type IDArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]ID).
func (IDArrayOutput) ElementType() reflect.Type {
	return iDArrayType
}

func (IDArrayOutput) isIDArray() {}

var iDMapType = reflect.TypeOf((*map[string]ID)(nil)).Elem()

// IDMapInput is an input type that accepts IDMap and IDMapOutput values.
type IDMapInput interface {
	Input

	// nolint: unused
	isIDMap()
}

// IDMap is an input type for map[string]IDInput values.
type IDMap map[string]IDInput

// ElementType returns the element type of this Input (map[string]ID).
func (IDMap) ElementType() reflect.Type {
	return iDMapType
}

func (IDMap) isIDMap() {}

// IDMapOutput is an Output that returns map[string]IDInput values.
type IDMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]ID).
func (IDMapOutput) ElementType() reflect.Type {
	return iDMapType
}

func (IDMapOutput) isIDMap() {}

var intType = reflect.TypeOf((*int)(nil)).Elem()

// IntInput is an input type that accepts Int and IntOutput values.
type IntInput interface {
	Input

	// nolint: unused
	isInt()
}

// Int is an input type for int values.
type Int int

// ElementType returns the element type of this Input (int).
func (Int) ElementType() reflect.Type {
	return intType
}

func (Int) isInt() {}

// IntOutput is an Output that returns int values.
type IntOutput struct{ *OutputState }

// ElementType returns the element type of this Output (int).
func (IntOutput) ElementType() reflect.Type {
	return intType
}

func (IntOutput) isInt() {}

var intArrayType = reflect.TypeOf((*[]int)(nil)).Elem()

// IntArrayInput is an input type that accepts IntArray and IntArrayOutput values.
type IntArrayInput interface {
	Input

	// nolint: unused
	isIntArray()
}

// IntArray is an input type for []IntInput values.
type IntArray []IntInput

// ElementType returns the element type of this Input ([]int).
func (IntArray) ElementType() reflect.Type {
	return intArrayType
}

func (IntArray) isIntArray() {}

// IntArrayOutput is an Output that returns []IntInput values.
type IntArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]int).
func (IntArrayOutput) ElementType() reflect.Type {
	return intArrayType
}

func (IntArrayOutput) isIntArray() {}

var intMapType = reflect.TypeOf((*map[string]int)(nil)).Elem()

// IntMapInput is an input type that accepts IntMap and IntMapOutput values.
type IntMapInput interface {
	Input

	// nolint: unused
	isIntMap()
}

// IntMap is an input type for map[string]IntInput values.
type IntMap map[string]IntInput

// ElementType returns the element type of this Input (map[string]int).
func (IntMap) ElementType() reflect.Type {
	return intMapType
}

func (IntMap) isIntMap() {}

// IntMapOutput is an Output that returns map[string]IntInput values.
type IntMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]int).
func (IntMapOutput) ElementType() reflect.Type {
	return intMapType
}

func (IntMapOutput) isIntMap() {}

var int16Type = reflect.TypeOf((*int16)(nil)).Elem()

// Int16Input is an input type that accepts Int16 and Int16Output values.
type Int16Input interface {
	Input

	// nolint: unused
	isInt16()
}

// Int16 is an input type for int16 values.
type Int16 int16

// ElementType returns the element type of this Input (int16).
func (Int16) ElementType() reflect.Type {
	return int16Type
}

func (Int16) isInt16() {}

// Int16Output is an Output that returns int16 values.
type Int16Output struct{ *OutputState }

// ElementType returns the element type of this Output (int16).
func (Int16Output) ElementType() reflect.Type {
	return int16Type
}

func (Int16Output) isInt16() {}

var int16ArrayType = reflect.TypeOf((*[]int16)(nil)).Elem()

// Int16ArrayInput is an input type that accepts Int16Array and Int16ArrayOutput values.
type Int16ArrayInput interface {
	Input

	// nolint: unused
	isInt16Array()
}

// Int16Array is an input type for []Int16Input values.
type Int16Array []Int16Input

// ElementType returns the element type of this Input ([]int16).
func (Int16Array) ElementType() reflect.Type {
	return int16ArrayType
}

func (Int16Array) isInt16Array() {}

// Int16ArrayOutput is an Output that returns []Int16Input values.
type Int16ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]int16).
func (Int16ArrayOutput) ElementType() reflect.Type {
	return int16ArrayType
}

func (Int16ArrayOutput) isInt16Array() {}

var int16MapType = reflect.TypeOf((*map[string]int16)(nil)).Elem()

// Int16MapInput is an input type that accepts Int16Map and Int16MapOutput values.
type Int16MapInput interface {
	Input

	// nolint: unused
	isInt16Map()
}

// Int16Map is an input type for map[string]Int16Input values.
type Int16Map map[string]Int16Input

// ElementType returns the element type of this Input (map[string]int16).
func (Int16Map) ElementType() reflect.Type {
	return int16MapType
}

func (Int16Map) isInt16Map() {}

// Int16MapOutput is an Output that returns map[string]Int16Input values.
type Int16MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]int16).
func (Int16MapOutput) ElementType() reflect.Type {
	return int16MapType
}

func (Int16MapOutput) isInt16Map() {}

var int32Type = reflect.TypeOf((*int32)(nil)).Elem()

// Int32Input is an input type that accepts Int32 and Int32Output values.
type Int32Input interface {
	Input

	// nolint: unused
	isInt32()
}

// Int32 is an input type for int32 values.
type Int32 int32

// ElementType returns the element type of this Input (int32).
func (Int32) ElementType() reflect.Type {
	return int32Type
}

func (Int32) isInt32() {}

// Int32Output is an Output that returns int32 values.
type Int32Output struct{ *OutputState }

// ElementType returns the element type of this Output (int32).
func (Int32Output) ElementType() reflect.Type {
	return int32Type
}

func (Int32Output) isInt32() {}

var int32ArrayType = reflect.TypeOf((*[]int32)(nil)).Elem()

// Int32ArrayInput is an input type that accepts Int32Array and Int32ArrayOutput values.
type Int32ArrayInput interface {
	Input

	// nolint: unused
	isInt32Array()
}

// Int32Array is an input type for []Int32Input values.
type Int32Array []Int32Input

// ElementType returns the element type of this Input ([]int32).
func (Int32Array) ElementType() reflect.Type {
	return int32ArrayType
}

func (Int32Array) isInt32Array() {}

// Int32ArrayOutput is an Output that returns []Int32Input values.
type Int32ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]int32).
func (Int32ArrayOutput) ElementType() reflect.Type {
	return int32ArrayType
}

func (Int32ArrayOutput) isInt32Array() {}

var int32MapType = reflect.TypeOf((*map[string]int32)(nil)).Elem()

// Int32MapInput is an input type that accepts Int32Map and Int32MapOutput values.
type Int32MapInput interface {
	Input

	// nolint: unused
	isInt32Map()
}

// Int32Map is an input type for map[string]Int32Input values.
type Int32Map map[string]Int32Input

// ElementType returns the element type of this Input (map[string]int32).
func (Int32Map) ElementType() reflect.Type {
	return int32MapType
}

func (Int32Map) isInt32Map() {}

// Int32MapOutput is an Output that returns map[string]Int32Input values.
type Int32MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]int32).
func (Int32MapOutput) ElementType() reflect.Type {
	return int32MapType
}

func (Int32MapOutput) isInt32Map() {}

var int64Type = reflect.TypeOf((*int64)(nil)).Elem()

// Int64Input is an input type that accepts Int64 and Int64Output values.
type Int64Input interface {
	Input

	// nolint: unused
	isInt64()
}

// Int64 is an input type for int64 values.
type Int64 int64

// ElementType returns the element type of this Input (int64).
func (Int64) ElementType() reflect.Type {
	return int64Type
}

func (Int64) isInt64() {}

// Int64Output is an Output that returns int64 values.
type Int64Output struct{ *OutputState }

// ElementType returns the element type of this Output (int64).
func (Int64Output) ElementType() reflect.Type {
	return int64Type
}

func (Int64Output) isInt64() {}

var int64ArrayType = reflect.TypeOf((*[]int64)(nil)).Elem()

// Int64ArrayInput is an input type that accepts Int64Array and Int64ArrayOutput values.
type Int64ArrayInput interface {
	Input

	// nolint: unused
	isInt64Array()
}

// Int64Array is an input type for []Int64Input values.
type Int64Array []Int64Input

// ElementType returns the element type of this Input ([]int64).
func (Int64Array) ElementType() reflect.Type {
	return int64ArrayType
}

func (Int64Array) isInt64Array() {}

// Int64ArrayOutput is an Output that returns []Int64Input values.
type Int64ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]int64).
func (Int64ArrayOutput) ElementType() reflect.Type {
	return int64ArrayType
}

func (Int64ArrayOutput) isInt64Array() {}

var int64MapType = reflect.TypeOf((*map[string]int64)(nil)).Elem()

// Int64MapInput is an input type that accepts Int64Map and Int64MapOutput values.
type Int64MapInput interface {
	Input

	// nolint: unused
	isInt64Map()
}

// Int64Map is an input type for map[string]Int64Input values.
type Int64Map map[string]Int64Input

// ElementType returns the element type of this Input (map[string]int64).
func (Int64Map) ElementType() reflect.Type {
	return int64MapType
}

func (Int64Map) isInt64Map() {}

// Int64MapOutput is an Output that returns map[string]Int64Input values.
type Int64MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]int64).
func (Int64MapOutput) ElementType() reflect.Type {
	return int64MapType
}

func (Int64MapOutput) isInt64Map() {}

var int8Type = reflect.TypeOf((*int8)(nil)).Elem()

// Int8Input is an input type that accepts Int8 and Int8Output values.
type Int8Input interface {
	Input

	// nolint: unused
	isInt8()
}

// Int8 is an input type for int8 values.
type Int8 int8

// ElementType returns the element type of this Input (int8).
func (Int8) ElementType() reflect.Type {
	return int8Type
}

func (Int8) isInt8() {}

// Int8Output is an Output that returns int8 values.
type Int8Output struct{ *OutputState }

// ElementType returns the element type of this Output (int8).
func (Int8Output) ElementType() reflect.Type {
	return int8Type
}

func (Int8Output) isInt8() {}

var int8ArrayType = reflect.TypeOf((*[]int8)(nil)).Elem()

// Int8ArrayInput is an input type that accepts Int8Array and Int8ArrayOutput values.
type Int8ArrayInput interface {
	Input

	// nolint: unused
	isInt8Array()
}

// Int8Array is an input type for []Int8Input values.
type Int8Array []Int8Input

// ElementType returns the element type of this Input ([]int8).
func (Int8Array) ElementType() reflect.Type {
	return int8ArrayType
}

func (Int8Array) isInt8Array() {}

// Int8ArrayOutput is an Output that returns []Int8Input values.
type Int8ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]int8).
func (Int8ArrayOutput) ElementType() reflect.Type {
	return int8ArrayType
}

func (Int8ArrayOutput) isInt8Array() {}

var int8MapType = reflect.TypeOf((*map[string]int8)(nil)).Elem()

// Int8MapInput is an input type that accepts Int8Map and Int8MapOutput values.
type Int8MapInput interface {
	Input

	// nolint: unused
	isInt8Map()
}

// Int8Map is an input type for map[string]Int8Input values.
type Int8Map map[string]Int8Input

// ElementType returns the element type of this Input (map[string]int8).
func (Int8Map) ElementType() reflect.Type {
	return int8MapType
}

func (Int8Map) isInt8Map() {}

// Int8MapOutput is an Output that returns map[string]Int8Input values.
type Int8MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]int8).
func (Int8MapOutput) ElementType() reflect.Type {
	return int8MapType
}

func (Int8MapOutput) isInt8Map() {}

var stringType = reflect.TypeOf((*string)(nil)).Elem()

// StringInput is an input type that accepts String and StringOutput values.
type StringInput interface {
	Input

	// nolint: unused
	isString()
}

// String is an input type for string values.
type String string

// ElementType returns the element type of this Input (string).
func (String) ElementType() reflect.Type {
	return stringType
}

func (String) isString() {}

// StringOutput is an Output that returns string values.
type StringOutput struct{ *OutputState }

// ElementType returns the element type of this Output (string).
func (StringOutput) ElementType() reflect.Type {
	return stringType
}

func (StringOutput) isString() {}

var stringArrayType = reflect.TypeOf((*[]string)(nil)).Elem()

// StringArrayInput is an input type that accepts StringArray and StringArrayOutput values.
type StringArrayInput interface {
	Input

	// nolint: unused
	isStringArray()
}

// StringArray is an input type for []StringInput values.
type StringArray []StringInput

// ElementType returns the element type of this Input ([]string).
func (StringArray) ElementType() reflect.Type {
	return stringArrayType
}

func (StringArray) isStringArray() {}

// StringArrayOutput is an Output that returns []StringInput values.
type StringArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]string).
func (StringArrayOutput) ElementType() reflect.Type {
	return stringArrayType
}

func (StringArrayOutput) isStringArray() {}

var stringMapType = reflect.TypeOf((*map[string]string)(nil)).Elem()

// StringMapInput is an input type that accepts StringMap and StringMapOutput values.
type StringMapInput interface {
	Input

	// nolint: unused
	isStringMap()
}

// StringMap is an input type for map[string]StringInput values.
type StringMap map[string]StringInput

// ElementType returns the element type of this Input (map[string]string).
func (StringMap) ElementType() reflect.Type {
	return stringMapType
}

func (StringMap) isStringMap() {}

// StringMapOutput is an Output that returns map[string]StringInput values.
type StringMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]string).
func (StringMapOutput) ElementType() reflect.Type {
	return stringMapType
}

func (StringMapOutput) isStringMap() {}

var uRNType = reflect.TypeOf((*URN)(nil)).Elem()

// URNInput is an input type that accepts URN and URNOutput values.
type URNInput interface {
	Input

	// nolint: unused
	isURN()
}

// ElementType returns the element type of this Input (URN).
func (URN) ElementType() reflect.Type {
	return uRNType
}

func (URN) isURN() {}

func (URN) isString() {}

// URNOutput is an Output that returns URN values.
type URNOutput struct{ *OutputState }

// ElementType returns the element type of this Output (URN).
func (URNOutput) ElementType() reflect.Type {
	return uRNType
}

func (URNOutput) isURN() {}

func (URNOutput) isString() {}

var uRNArrayType = reflect.TypeOf((*[]URN)(nil)).Elem()

// URNArrayInput is an input type that accepts URNArray and URNArrayOutput values.
type URNArrayInput interface {
	Input

	// nolint: unused
	isURNArray()
}

// URNArray is an input type for []URNInput values.
type URNArray []URNInput

// ElementType returns the element type of this Input ([]URN).
func (URNArray) ElementType() reflect.Type {
	return uRNArrayType
}

func (URNArray) isURNArray() {}

// URNArrayOutput is an Output that returns []URNInput values.
type URNArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]URN).
func (URNArrayOutput) ElementType() reflect.Type {
	return uRNArrayType
}

func (URNArrayOutput) isURNArray() {}

var uRNMapType = reflect.TypeOf((*map[string]URN)(nil)).Elem()

// URNMapInput is an input type that accepts URNMap and URNMapOutput values.
type URNMapInput interface {
	Input

	// nolint: unused
	isURNMap()
}

// URNMap is an input type for map[string]URNInput values.
type URNMap map[string]URNInput

// ElementType returns the element type of this Input (map[string]URN).
func (URNMap) ElementType() reflect.Type {
	return uRNMapType
}

func (URNMap) isURNMap() {}

// URNMapOutput is an Output that returns map[string]URNInput values.
type URNMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]URN).
func (URNMapOutput) ElementType() reflect.Type {
	return uRNMapType
}

func (URNMapOutput) isURNMap() {}

var uintType = reflect.TypeOf((*uint)(nil)).Elem()

// UintInput is an input type that accepts Uint and UintOutput values.
type UintInput interface {
	Input

	// nolint: unused
	isUint()
}

// Uint is an input type for uint values.
type Uint uint

// ElementType returns the element type of this Input (uint).
func (Uint) ElementType() reflect.Type {
	return uintType
}

func (Uint) isUint() {}

// UintOutput is an Output that returns uint values.
type UintOutput struct{ *OutputState }

// ElementType returns the element type of this Output (uint).
func (UintOutput) ElementType() reflect.Type {
	return uintType
}

func (UintOutput) isUint() {}

var uintArrayType = reflect.TypeOf((*[]uint)(nil)).Elem()

// UintArrayInput is an input type that accepts UintArray and UintArrayOutput values.
type UintArrayInput interface {
	Input

	// nolint: unused
	isUintArray()
}

// UintArray is an input type for []UintInput values.
type UintArray []UintInput

// ElementType returns the element type of this Input ([]uint).
func (UintArray) ElementType() reflect.Type {
	return uintArrayType
}

func (UintArray) isUintArray() {}

// UintArrayOutput is an Output that returns []UintInput values.
type UintArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]uint).
func (UintArrayOutput) ElementType() reflect.Type {
	return uintArrayType
}

func (UintArrayOutput) isUintArray() {}

var uintMapType = reflect.TypeOf((*map[string]uint)(nil)).Elem()

// UintMapInput is an input type that accepts UintMap and UintMapOutput values.
type UintMapInput interface {
	Input

	// nolint: unused
	isUintMap()
}

// UintMap is an input type for map[string]UintInput values.
type UintMap map[string]UintInput

// ElementType returns the element type of this Input (map[string]uint).
func (UintMap) ElementType() reflect.Type {
	return uintMapType
}

func (UintMap) isUintMap() {}

// UintMapOutput is an Output that returns map[string]UintInput values.
type UintMapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]uint).
func (UintMapOutput) ElementType() reflect.Type {
	return uintMapType
}

func (UintMapOutput) isUintMap() {}

var uint16Type = reflect.TypeOf((*uint16)(nil)).Elem()

// Uint16Input is an input type that accepts Uint16 and Uint16Output values.
type Uint16Input interface {
	Input

	// nolint: unused
	isUint16()
}

// Uint16 is an input type for uint16 values.
type Uint16 uint16

// ElementType returns the element type of this Input (uint16).
func (Uint16) ElementType() reflect.Type {
	return uint16Type
}

func (Uint16) isUint16() {}

// Uint16Output is an Output that returns uint16 values.
type Uint16Output struct{ *OutputState }

// ElementType returns the element type of this Output (uint16).
func (Uint16Output) ElementType() reflect.Type {
	return uint16Type
}

func (Uint16Output) isUint16() {}

var uint16ArrayType = reflect.TypeOf((*[]uint16)(nil)).Elem()

// Uint16ArrayInput is an input type that accepts Uint16Array and Uint16ArrayOutput values.
type Uint16ArrayInput interface {
	Input

	// nolint: unused
	isUint16Array()
}

// Uint16Array is an input type for []Uint16Input values.
type Uint16Array []Uint16Input

// ElementType returns the element type of this Input ([]uint16).
func (Uint16Array) ElementType() reflect.Type {
	return uint16ArrayType
}

func (Uint16Array) isUint16Array() {}

// Uint16ArrayOutput is an Output that returns []Uint16Input values.
type Uint16ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]uint16).
func (Uint16ArrayOutput) ElementType() reflect.Type {
	return uint16ArrayType
}

func (Uint16ArrayOutput) isUint16Array() {}

var uint16MapType = reflect.TypeOf((*map[string]uint16)(nil)).Elem()

// Uint16MapInput is an input type that accepts Uint16Map and Uint16MapOutput values.
type Uint16MapInput interface {
	Input

	// nolint: unused
	isUint16Map()
}

// Uint16Map is an input type for map[string]Uint16Input values.
type Uint16Map map[string]Uint16Input

// ElementType returns the element type of this Input (map[string]uint16).
func (Uint16Map) ElementType() reflect.Type {
	return uint16MapType
}

func (Uint16Map) isUint16Map() {}

// Uint16MapOutput is an Output that returns map[string]Uint16Input values.
type Uint16MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]uint16).
func (Uint16MapOutput) ElementType() reflect.Type {
	return uint16MapType
}

func (Uint16MapOutput) isUint16Map() {}

var uint32Type = reflect.TypeOf((*uint32)(nil)).Elem()

// Uint32Input is an input type that accepts Uint32 and Uint32Output values.
type Uint32Input interface {
	Input

	// nolint: unused
	isUint32()
}

// Uint32 is an input type for uint32 values.
type Uint32 uint32

// ElementType returns the element type of this Input (uint32).
func (Uint32) ElementType() reflect.Type {
	return uint32Type
}

func (Uint32) isUint32() {}

// Uint32Output is an Output that returns uint32 values.
type Uint32Output struct{ *OutputState }

// ElementType returns the element type of this Output (uint32).
func (Uint32Output) ElementType() reflect.Type {
	return uint32Type
}

func (Uint32Output) isUint32() {}

var uint32ArrayType = reflect.TypeOf((*[]uint32)(nil)).Elem()

// Uint32ArrayInput is an input type that accepts Uint32Array and Uint32ArrayOutput values.
type Uint32ArrayInput interface {
	Input

	// nolint: unused
	isUint32Array()
}

// Uint32Array is an input type for []Uint32Input values.
type Uint32Array []Uint32Input

// ElementType returns the element type of this Input ([]uint32).
func (Uint32Array) ElementType() reflect.Type {
	return uint32ArrayType
}

func (Uint32Array) isUint32Array() {}

// Uint32ArrayOutput is an Output that returns []Uint32Input values.
type Uint32ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]uint32).
func (Uint32ArrayOutput) ElementType() reflect.Type {
	return uint32ArrayType
}

func (Uint32ArrayOutput) isUint32Array() {}

var uint32MapType = reflect.TypeOf((*map[string]uint32)(nil)).Elem()

// Uint32MapInput is an input type that accepts Uint32Map and Uint32MapOutput values.
type Uint32MapInput interface {
	Input

	// nolint: unused
	isUint32Map()
}

// Uint32Map is an input type for map[string]Uint32Input values.
type Uint32Map map[string]Uint32Input

// ElementType returns the element type of this Input (map[string]uint32).
func (Uint32Map) ElementType() reflect.Type {
	return uint32MapType
}

func (Uint32Map) isUint32Map() {}

// Uint32MapOutput is an Output that returns map[string]Uint32Input values.
type Uint32MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]uint32).
func (Uint32MapOutput) ElementType() reflect.Type {
	return uint32MapType
}

func (Uint32MapOutput) isUint32Map() {}

var uint64Type = reflect.TypeOf((*uint64)(nil)).Elem()

// Uint64Input is an input type that accepts Uint64 and Uint64Output values.
type Uint64Input interface {
	Input

	// nolint: unused
	isUint64()
}

// Uint64 is an input type for uint64 values.
type Uint64 uint64

// ElementType returns the element type of this Input (uint64).
func (Uint64) ElementType() reflect.Type {
	return uint64Type
}

func (Uint64) isUint64() {}

// Uint64Output is an Output that returns uint64 values.
type Uint64Output struct{ *OutputState }

// ElementType returns the element type of this Output (uint64).
func (Uint64Output) ElementType() reflect.Type {
	return uint64Type
}

func (Uint64Output) isUint64() {}

var uint64ArrayType = reflect.TypeOf((*[]uint64)(nil)).Elem()

// Uint64ArrayInput is an input type that accepts Uint64Array and Uint64ArrayOutput values.
type Uint64ArrayInput interface {
	Input

	// nolint: unused
	isUint64Array()
}

// Uint64Array is an input type for []Uint64Input values.
type Uint64Array []Uint64Input

// ElementType returns the element type of this Input ([]uint64).
func (Uint64Array) ElementType() reflect.Type {
	return uint64ArrayType
}

func (Uint64Array) isUint64Array() {}

// Uint64ArrayOutput is an Output that returns []Uint64Input values.
type Uint64ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]uint64).
func (Uint64ArrayOutput) ElementType() reflect.Type {
	return uint64ArrayType
}

func (Uint64ArrayOutput) isUint64Array() {}

var uint64MapType = reflect.TypeOf((*map[string]uint64)(nil)).Elem()

// Uint64MapInput is an input type that accepts Uint64Map and Uint64MapOutput values.
type Uint64MapInput interface {
	Input

	// nolint: unused
	isUint64Map()
}

// Uint64Map is an input type for map[string]Uint64Input values.
type Uint64Map map[string]Uint64Input

// ElementType returns the element type of this Input (map[string]uint64).
func (Uint64Map) ElementType() reflect.Type {
	return uint64MapType
}

func (Uint64Map) isUint64Map() {}

// Uint64MapOutput is an Output that returns map[string]Uint64Input values.
type Uint64MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]uint64).
func (Uint64MapOutput) ElementType() reflect.Type {
	return uint64MapType
}

func (Uint64MapOutput) isUint64Map() {}

var uint8Type = reflect.TypeOf((*uint8)(nil)).Elem()

// Uint8Input is an input type that accepts Uint8 and Uint8Output values.
type Uint8Input interface {
	Input

	// nolint: unused
	isUint8()
}

// Uint8 is an input type for uint8 values.
type Uint8 uint8

// ElementType returns the element type of this Input (uint8).
func (Uint8) ElementType() reflect.Type {
	return uint8Type
}

func (Uint8) isUint8() {}

// Uint8Output is an Output that returns uint8 values.
type Uint8Output struct{ *OutputState }

// ElementType returns the element type of this Output (uint8).
func (Uint8Output) ElementType() reflect.Type {
	return uint8Type
}

func (Uint8Output) isUint8() {}

var uint8ArrayType = reflect.TypeOf((*[]uint8)(nil)).Elem()

// Uint8ArrayInput is an input type that accepts Uint8Array and Uint8ArrayOutput values.
type Uint8ArrayInput interface {
	Input

	// nolint: unused
	isUint8Array()
}

// Uint8Array is an input type for []Uint8Input values.
type Uint8Array []Uint8Input

// ElementType returns the element type of this Input ([]uint8).
func (Uint8Array) ElementType() reflect.Type {
	return uint8ArrayType
}

func (Uint8Array) isUint8Array() {}

// Uint8ArrayOutput is an Output that returns []Uint8Input values.
type Uint8ArrayOutput struct{ *OutputState }

// ElementType returns the element type of this Output ([]uint8).
func (Uint8ArrayOutput) ElementType() reflect.Type {
	return uint8ArrayType
}

func (Uint8ArrayOutput) isUint8Array() {}

var uint8MapType = reflect.TypeOf((*map[string]uint8)(nil)).Elem()

// Uint8MapInput is an input type that accepts Uint8Map and Uint8MapOutput values.
type Uint8MapInput interface {
	Input

	// nolint: unused
	isUint8Map()
}

// Uint8Map is an input type for map[string]Uint8Input values.
type Uint8Map map[string]Uint8Input

// ElementType returns the element type of this Input (map[string]uint8).
func (Uint8Map) ElementType() reflect.Type {
	return uint8MapType
}

func (Uint8Map) isUint8Map() {}

// Uint8MapOutput is an Output that returns map[string]Uint8Input values.
type Uint8MapOutput struct{ *OutputState }

// ElementType returns the element type of this Output (map[string]uint8).
func (Uint8MapOutput) ElementType() reflect.Type {
	return uint8MapType
}

func (Uint8MapOutput) isUint8Map() {}

func init() {
	RegisterOutputType(AnyOutput{})
	RegisterOutputType(AnyArrayOutput{})
	RegisterOutputType(AnyMapOutput{})
	RegisterOutputType(ArchiveOutput{})
	RegisterOutputType(ArchiveArrayOutput{})
	RegisterOutputType(ArchiveMapOutput{})
	RegisterOutputType(AssetOutput{})
	RegisterOutputType(AssetArrayOutput{})
	RegisterOutputType(AssetMapOutput{})
	RegisterOutputType(AssetOrArchiveOutput{})
	RegisterOutputType(AssetOrArchiveArrayOutput{})
	RegisterOutputType(AssetOrArchiveMapOutput{})
	RegisterOutputType(BoolOutput{})
	RegisterOutputType(BoolArrayOutput{})
	RegisterOutputType(BoolMapOutput{})
	RegisterOutputType(Float32Output{})
	RegisterOutputType(Float32ArrayOutput{})
	RegisterOutputType(Float32MapOutput{})
	RegisterOutputType(Float64Output{})
	RegisterOutputType(Float64ArrayOutput{})
	RegisterOutputType(Float64MapOutput{})
	RegisterOutputType(IDOutput{})
	RegisterOutputType(IDArrayOutput{})
	RegisterOutputType(IDMapOutput{})
	RegisterOutputType(IntOutput{})
	RegisterOutputType(IntArrayOutput{})
	RegisterOutputType(IntMapOutput{})
	RegisterOutputType(Int16Output{})
	RegisterOutputType(Int16ArrayOutput{})
	RegisterOutputType(Int16MapOutput{})
	RegisterOutputType(Int32Output{})
	RegisterOutputType(Int32ArrayOutput{})
	RegisterOutputType(Int32MapOutput{})
	RegisterOutputType(Int64Output{})
	RegisterOutputType(Int64ArrayOutput{})
	RegisterOutputType(Int64MapOutput{})
	RegisterOutputType(Int8Output{})
	RegisterOutputType(Int8ArrayOutput{})
	RegisterOutputType(Int8MapOutput{})
	RegisterOutputType(StringOutput{})
	RegisterOutputType(StringArrayOutput{})
	RegisterOutputType(StringMapOutput{})
	RegisterOutputType(URNOutput{})
	RegisterOutputType(URNArrayOutput{})
	RegisterOutputType(URNMapOutput{})
	RegisterOutputType(UintOutput{})
	RegisterOutputType(UintArrayOutput{})
	RegisterOutputType(UintMapOutput{})
	RegisterOutputType(Uint16Output{})
	RegisterOutputType(Uint16ArrayOutput{})
	RegisterOutputType(Uint16MapOutput{})
	RegisterOutputType(Uint32Output{})
	RegisterOutputType(Uint32ArrayOutput{})
	RegisterOutputType(Uint32MapOutput{})
	RegisterOutputType(Uint64Output{})
	RegisterOutputType(Uint64ArrayOutput{})
	RegisterOutputType(Uint64MapOutput{})
	RegisterOutputType(Uint8Output{})
	RegisterOutputType(Uint8ArrayOutput{})
	RegisterOutputType(Uint8MapOutput{})
}

func (out IDOutput) awaitID(ctx context.Context) (ID, bool, error) {
	id, known, err := out.await(ctx)
	if !known || err != nil {
		return "", known, err
	}
	return ID(convert(id, stringType).(string)), true, nil
}

func (out URNOutput) awaitURN(ctx context.Context) (URN, bool, error) {
	id, known, err := out.await(ctx)
	if !known || err != nil {
		return "", known, err
	}
	return URN(convert(id, stringType).(string)), true, nil
}

func convert(v interface{}, to reflect.Type) interface{} {
	rv := reflect.ValueOf(v)
	if !rv.Type().ConvertibleTo(to) {
		panic(errors.Errorf("cannot convert output value of type %s to %s", rv.Type(), to))
	}
	return rv.Convert(to).Interface()
}
