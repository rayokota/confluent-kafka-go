/**
 * Copyright 2024 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package avrov2

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/hamba/avro/v2"
	"github.com/modern-go/reflect2"
	"reflect"
	"strings"
)

func transform(ctx serde.RuleContext, resolver *avro.TypeResolver, schema avro.Schema, msg *reflect.Value,
	fieldTransform serde.FieldTransform) (*reflect.Value, error) {
	if schema == nil {
		return msg, nil
	}
	fieldCtx := ctx.CurrentField()
	if fieldCtx != nil {
		fieldCtx.Type = getType(schema)
	}
	switch schema.(type) {
	case *avro.UnionSchema:
		subschema, err := resolveUnion(resolver, schema, msg)
		if err != nil {
			return nil, err
		}
		return transform(ctx, resolver, subschema, msg, fieldTransform)
		/*
			case *avro.ArraySchema:
				s.items = walkSchema(s.items, fn)
				return msg, nil
			case *avro.MapSchema:
				s.values = walkSchema(s.values, fn)
				return msg, nil

		*/
	case *avro.RecordSchema:
		if msg == nil {
			return msg, nil
		}
		recordSchema := schema.(*avro.RecordSchema)
		for _, f := range recordSchema.Fields() {
			fullName := recordSchema.FullName() + "." + f.Name()
			defer ctx.LeaveField()
			ctx.EnterField(deref(msg).Interface(), fullName, f.Name(), getType(f.Type()), getInlineTags(f))
			field, err := getField(msg, f.Name())
			if err != nil {
				return nil, err
			}
			newVal, err := transform(ctx, resolver, f.Type(), field, fieldTransform)
			if err != nil {
				return nil, err
			}
			if ctx.Rule.Kind == "CONDITION" {
				// TODO test
				newBool := deref(newVal)
				if newBool.Kind() == reflect.Bool && !newBool.Bool() {
					return nil, serde.RuleConditionErr{
						Rule: ctx.Rule,
					}
				}
			}
			err = setField(field, newVal)
			if err != nil {
				return nil, err
			}
		}
		return msg, nil
	default:
		if fieldCtx != nil {
			ruleTags := ctx.Rule.Tags
			if len(ruleTags) == 0 || !disjoint(ruleTags, fieldCtx.Tags) {
				val := deref(msg).Interface()
				newVal, err := fieldTransform.Transform(ctx, *fieldCtx, val)
				if err != nil {
					return nil, err
				}
				v := reflect.ValueOf(newVal)
				return &v, nil
			}
		}
		return msg, nil
	}
}

func getType(schema avro.Schema) serde.FieldType {
	switch schema.Type() {
	case avro.Record:
		return serde.TypeRecord
	case avro.Enum:
		return serde.TypeEnum
	case avro.Array:
		return serde.TypeArray
	case avro.Map:
		return serde.TypeMap
	case avro.Union:
		return serde.TypeCombined
	case avro.Fixed:
		return serde.TypeFixed
	case avro.String:
		return serde.TypeString
	case avro.Bytes:
		return serde.TypeBytes
	case avro.Int:
		return serde.TypeInt
	case avro.Long:
		return serde.TypeLong
	case avro.Float:
		return serde.TypeFloat
	case avro.Double:
		return serde.TypeDouble
	case avro.Boolean:
		return serde.TypeBoolean
	case avro.Null:
		return serde.TypeNull
	default:
		return serde.TypeNull
	}
}

func getInlineTags(field *avro.Field) []string {
	prop := field.Prop("confluent:tags")
	val, ok := prop.([]interface{})
	if ok {
		tags := make([]string, len(val))
		for i, v := range val {
			tags[i] = fmt.Sprint(v)
		}
		return tags
	}
	return []string{}
}

func disjoint(slice1 []string, map1 map[string]bool) bool {
	for _, v := range slice1 {
		if map1[v] {
			return false
		}
	}
	return true
}

func getField(msg *reflect.Value, name string) (*reflect.Value, error) {
	fieldVal := deref(msg).FieldByName(name)
	return &fieldVal, nil
}

func setField(field *reflect.Value, value *reflect.Value) error {
	v := field
	if !v.CanSet() {
		return fmt.Errorf("cannot assign to the given field")
	}
	v.Set(*value)
	return nil
}

func resolveUnion(resolver *avro.TypeResolver, schema avro.Schema, msg *reflect.Value) (avro.Schema, error) {
	union := schema.(*avro.UnionSchema)
	// TODO test
	val := deref(msg).Interface()
	typ := reflect2.TypeOf(val)
	names, err := resolver.Name(typ)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if idx := strings.Index(name, ":"); idx > 0 {
			name = name[:idx]
		}

		schema, _ = union.Types().Get(name)
		if schema != nil {
			return schema, nil
		}
	}
	return nil, fmt.Errorf("avro: unknown union type %s", names[0])
}

func deref(val *reflect.Value) *reflect.Value {
	if val.Kind() == reflect.Pointer {
		v := val.Elem()
		return &v
	}
	return val
}
