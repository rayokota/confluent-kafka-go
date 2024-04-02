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

func transform(ctx serde.RuleContext, resolver *avro.TypeResolver, schema avro.Schema, msg interface{},
	fieldTransform serde.FieldTransform) (interface{}, error) {
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
			ctx.EnterField(msg, fullName, f.Name(), getType(f.Type()), getInlineTags(f))
			val := getField(msg, f.Name())
			newVal, err := transform(ctx, resolver, f.Type(), val, fieldTransform)
			if err != nil {
				return nil, err
			}
			if ctx.Rule.Kind == "CONDITION" {
				newBool, ok := newVal.(bool)
				if ok && !newBool {
					return nil, serde.RuleConditionErr{
						Rule: ctx.Rule,
					}
				}
			}
			err = setField(msg, f.Name(), newVal)
			if err != nil {
				return nil, err
			}
		}
		return msg, nil
	default:
		if fieldCtx != nil {
			ruleTags := ctx.Rule.Tags
			if len(ruleTags) == 0 || !disjoint(ruleTags, fieldCtx.Tags) {
				return fieldTransform.Transform(ctx, *fieldCtx, msg)
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

func getField(msg interface{}, name string) interface{} {
	v := reflect.ValueOf(msg)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	fieldVal := v.FieldByName(name)
	return fieldVal.Interface()
}

func setField(msg interface{}, name string, value interface{}) error {
	v := reflect.ValueOf(msg)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	fieldVal := v.FieldByName(name)
	fieldVal.Set(reflect.ValueOf(value))
	return nil
}

func resolveUnion(resolver *avro.TypeResolver, schema avro.Schema, msg interface{}) (avro.Schema, error) {
	union := schema.(*avro.UnionSchema)
	typ := reflect2.TypeOf(msg)
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

func doFieldTransform(ctx serde.RuleContext, msg interface{}, fieldTransform serde.FieldTransform,
	fieldCtx *serde.FieldContext) (interface{}, error) {
	return nil, nil
}
