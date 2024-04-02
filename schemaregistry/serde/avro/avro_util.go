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

package avro

import (
	"github.com/actgardner/gogen-avro/v10/schema"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
)

func transform(ctx serde.RuleContext, avroType schema.AvroType, msg interface{},
	fieldTransform serde.FieldTransform) (interface{}, error) {
	return nil, nil
}

/*
	switch avroType.(type) {
	case *schema.UnionField:
		templateDef = UnionTemplate
	case *schema.ArrayField:
		templateDef = ArrayTemplate
	case *schema.MapField:
		templateDef = MapTemplate
	case *schema.RecordDefinition:
		templateDef = RecordTemplate
	case *schema.BytesField:
		templateDef = BytesTemplate
	case *schema.EnumDefinition:
		templateDef = EnumTemplate
	case *schema.FixedDefinition:
		templateDef = FixedTemplate
	default:
		return "", NoTemplateForType

	}

	v := reflect.ValueOf(msg)
	if descriptor == nil {
		return msg, nil
	}
	if v.Kind() == reflect.Slice {
		var result []interface{}
		for i := 0; i < v.Len(); i++ {
			newmsg, err := transform(ctx, descriptor, v.Index(i).Interface(), fieldTransform)
			if err != nil {
				return nil, err
			}
			result = append(result, newmsg)
		}
		return result, nil
	}
	if v.Kind() == reflect.Map {
		return msg, nil
	}
	m, ok := msg.(proto.Message)
	if ok {
		desc := descriptor.(protoreflect.MessageDescriptor)
		clone := proto.Clone(m)
		fields := clone.ProtoReflect().Descriptor().Fields()
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			schemaFd := desc.Fields().ByName(fd.Name())
			defer ctx.LeaveField()
			ctx.EnterField(msg, string(fd.FullName()), string(fd.Name()), getType(fd), getInlineTags(schemaFd))
			value := clone.ProtoReflect().Get(fd)
			d := desc
			md, ok := desc.(protoreflect.MessageDescriptor)
			if ok {
				// Pass the schema-based descriptor which has the tags
				d = md
			}
			newValue, err := transform(ctx, d, value, fieldTransform)
			if err != nil {
				return nil, err
			}
			newProtoValue := newValue.(protoreflect.Value)
			if ctx.Rule.Kind == "CONDITION" {
				i := newProtoValue.Interface()
				newBool, ok := i.(bool)
				if ok && !newBool {
					return nil, serde.RuleConditionErr{
						Rule: ctx.Rule,
					}
				}

			}
			clone.ProtoReflect().Set(fd, newProtoValue)
		}
		return clone, nil
	}
	fieldCtx := ctx.CurrentField()
	if fieldCtx != nil {
		ruleTags := ctx.Rule.Tags
		if (ruleTags == nil && len(ruleTags) == 0) || !disjoint(ruleTags, fieldCtx.Tags) {
			val := msg.(protoreflect.Value)
			newVal, err := fieldTransform.Transform(ctx, *fieldCtx, val.Interface())
			if err != nil {
				return nil, err
			}
			return protoreflect.ValueOf(newVal), nil
		}
	}
	return msg, nil
}

func getType(fd protoreflect.FieldDescriptor) serde.FieldType {
	if fd.IsMap() {
		return serde.TypeMap
	}
	switch fd.Kind() {
	case protoreflect.MessageKind:
		return serde.TypeRecord
	case protoreflect.EnumKind:
		return serde.TypeEnum
	case protoreflect.StringKind:
		return serde.TypeString
	case protoreflect.BytesKind:
		return serde.TypeBytes
	case protoreflect.Int32Kind, protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		return serde.TypeInt
	case protoreflect.Int64Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		return serde.TypeLong
	case protoreflect.FloatKind:
		return serde.TypeFloat
	case protoreflect.DoubleKind:
		return serde.TypeDouble
	case protoreflect.BoolKind:
		return serde.TypeBoolean
	default:
		return serde.TypeNull
	}

}

func getInlineTags(fd protoreflect.FieldDescriptor) []string {
	options := fd.Options()
	if proto.HasExtension(options, confluent.E_FieldMeta) {
		option := proto.GetExtension(options, confluent.E_FieldMeta)
		meta, ok := option.(*confluent.Meta)
		if ok {
			return meta.Tags
		}
	}
	return nil
}

func disjoint(slice1 []string, map1 map[string]bool) bool {
	for _, v := range slice1 {
		if map1[v] {
			return false
		}
	}
	return true
}


*/
