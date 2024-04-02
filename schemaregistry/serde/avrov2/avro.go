/**
 * Copyright 2022 Confluent Inc.
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
	"encoding"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/hamba/avro/v2"
	"reflect"
	"strings"
	"time"
)

// Serializer represents a generic Avro serializer
type Serializer struct {
	serde.BaseSerializer
}

// Deserializer represents a generic Avro deserializer
type Deserializer struct {
	serde.BaseDeserializer
}

var _ serde.Serializer = new(Serializer)
var _ serde.Deserializer = new(Deserializer)

// NewSerializer creates an Avro serializer for generic objects
func NewSerializer(client schemaregistry.Client, serdeType serde.Type, conf *SerializerConfig) (*Serializer, error) {
	s := &Serializer{}
	err := s.ConfigureSerializer(client, serdeType, &conf.SerializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Serialize implements serialization of generic Avro data
func (s *Serializer) Serialize(topic string, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	val := reflect.ValueOf(msg)
	if val.Kind() == reflect.Ptr {
		// avro.TypeOf expects an interface containing a non-pointer
		msg = val.Elem().Interface()
	}
	avroType, err := StructToSchema(reflect.TypeOf(msg))
	if err != nil {
		return nil, err
	}
	info := schemaregistry.SchemaInfo{
		Schema: avroType.String(),
	}
	id, err := s.GetID(topic, msg, &info)
	if err != nil {
		return nil, err
	}
	msgBytes, err := avro.Marshal(avroType, msg)
	if err != nil {
		return nil, err
	}
	payload, err := s.WriteBytes(id, msgBytes)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// NewDeserializer creates an Avro deserializer for generic objects
func NewDeserializer(client schemaregistry.Client, serdeType serde.Type, conf *DeserializerConfig) (*Deserializer, error) {
	s := &Deserializer{}
	err := s.ConfigureDeserializer(client, serdeType, &conf.DeserializerConfig)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Deserialize implements deserialization of generic Avro data
func (s *Deserializer) Deserialize(topic string, payload []byte) (interface{}, error) {
	if payload == nil {
		return nil, nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return nil, err
	}
	writer, name, err := s.toType(info)
	if err != nil {
		return nil, err
	}
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return nil, err
	}
	msg, err := s.MessageFactory(subject, name)
	if err != nil {
		return nil, err
	}
	err = avro.Unmarshal(writer, payload[5:], msg)
	return msg, err
}

// DeserializeInto implements deserialization of generic Avro data to the given object
func (s *Deserializer) DeserializeInto(topic string, payload []byte, msg interface{}) error {
	if payload == nil {
		return nil
	}
	info, err := s.GetSchema(topic, payload)
	if err != nil {
		return err
	}
	writer, _, err := s.toType(info)
	if err != nil {
		return err
	}
	err = avro.Unmarshal(writer, payload[5:], msg)
	return err
}

func (s *Deserializer) toType(schema schemaregistry.SchemaInfo) (avro.Schema, string, error) {
	avroType, err := resolveAvroReferences(s.Client, schema)
	name := ""
	named, ok := avroType.(avro.NamedSchema)
	if ok {
		name = named.FullName()
	}
	return avroType, name, err
}

func resolveAvroReferences(c schemaregistry.Client, schema schemaregistry.SchemaInfo) (avro.Schema, error) {
	for _, ref := range schema.References {
		metadata, err := c.GetSchemaMetadata(ref.Subject, ref.Version)
		if err != nil {
			return nil, err
		}
		info := schemaregistry.SchemaInfo{
			Schema:     metadata.Schema,
			SchemaType: metadata.SchemaType,
			References: metadata.References,
			Metadata:   metadata.Metadata,
			Ruleset:    metadata.Ruleset,
		}
		_, err = resolveAvroReferences(c, info)
		if err != nil {
			return nil, err
		}

	}
	sType, err := avro.Parse(schema.Schema)
	if err != nil {
		return nil, err
	}
	return sType, nil
}

func StructToSchema(t reflect.Type, tags ...reflect.StructTag) (avro.Schema, error) {
	var schFields []*avro.Field
	switch t.Kind() {
	case reflect.Struct:
		if t.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMillis)), nil
		}
		if t.Implements(reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()) {
			subtype := strings.Split(t.String(), ".")
			return avro.NewPrimitiveSchema(avro.String, nil, avro.WithProps(map[string]any{"subtype": strings.ToLower(subtype[len(subtype)-1])})), nil
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			s, err := StructToSchema(f.Type, f.Tag)
			if err != nil {
				return nil, fmt.Errorf("StructToSchema: %w", err)
			}
			fName := f.Tag.Get("avro")
			if len(fName) == 0 {
				fName = f.Name
			} else if fName == "-" {
				continue
			}
			schField, err := avro.NewField(fName, s)
			if err != nil {
				return nil, fmt.Errorf("avro.NewField: %w", err)
			}
			schFields = append(schFields, schField)
		}
		name := t.Name()
		if len(name) == 0 {
			name = "anonymous"
		}
		return avro.NewRecordSchema(name, "", schFields)
	case reflect.Map:
		s, err := StructToSchema(t.Elem(), tags...)
		if err != nil {
			return nil, fmt.Errorf("StructToSchema: %w", err)
		}
		return avro.NewMapSchema(s), nil
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			if strings.Contains(strings.ToLower(t.Elem().String()), "decimal") {
				return avro.NewPrimitiveSchema(avro.Bytes, avro.NewPrimitiveLogicalSchema(avro.Decimal)), nil
			}
			if strings.Contains(strings.ToLower(t.Elem().String()), "uuid") {
				return avro.NewPrimitiveSchema(avro.String, avro.NewPrimitiveLogicalSchema(avro.UUID)), nil
			}
			return avro.NewPrimitiveSchema(avro.Bytes, nil), nil
		}
		s, err := StructToSchema(t.Elem(), tags...)
		if err != nil {
			return nil, fmt.Errorf("StructToSchema: %w", err)
		}
		return avro.NewArraySchema(s), nil
	case reflect.Pointer:
		n := avro.NewPrimitiveSchema(avro.Null, nil)
		s, err := StructToSchema(t.Elem(), tags...)
		if err != nil {
			return nil, fmt.Errorf("StructToSchema: %w", err)
		}
		union, err := avro.NewUnionSchema([]avro.Schema{n, s})
		if err != nil {
			return nil, fmt.Errorf("avro.NewUnionSchema: %v, type: %s", err, s.String())
		}
		return union, nil
	case reflect.Bool:
		return avro.NewPrimitiveSchema(avro.Boolean, nil), nil
	case reflect.Uint8, reflect.Int8:
		return avro.NewPrimitiveSchema(avro.Bytes, nil), nil
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint16, reflect.Uint32:
		if strings.Contains(strings.ToLower(t.String()), "date") {
			return avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date)), nil
		}
		if strings.Contains(strings.ToLower(t.String()), "time") {
			return avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.TimeMillis)), nil
		}
		return avro.NewPrimitiveSchema(avro.Int, nil), nil
	case reflect.Int64, reflect.Uint64:
		if strings.Contains(strings.ToLower(t.String()), "duration") {
			return avro.NewPrimitiveSchema(avro.Fixed, avro.NewPrimitiveLogicalSchema(avro.Duration)), nil
		}
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case reflect.Float32:
		return avro.NewPrimitiveSchema(avro.Float, nil), nil
	case reflect.Float64:
		return avro.NewPrimitiveSchema(avro.Double, nil), nil
	case reflect.String:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	default:
		return nil, fmt.Errorf("unknown type %s", t.Kind().String())
	}
}
