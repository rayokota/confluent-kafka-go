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

package serde

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

// Type represents the type of Serde
type Type = int

const (
	// KeySerde denotes a key Serde
	KeySerde = 1
	// ValueSerde denotes a value Serde
	ValueSerde = 2
)

const (
	// EnableValidation enables validation
	EnableValidation = true
	// DisableValidation disables validation
	DisableValidation = false
)

// RuleMode represents the rule mode
type RuleMode = int

const (
	// Upgrade denotes upgrade mode
	Upgrade = 1
	// Downgrade denotes downgrade mode
	Downgrade = 2
	// UpDown denotes upgrade/downgrade mode
	UpDown = 3
	// Write denotes write mode
	Write = 4
	// Read denotes read mode
	Read = 5
	// WriteRead denotes write/read mode
	WriteRead = 6
)

var modes = map[string]RuleMode{
	"UPGRADE":   Upgrade,
	"DOWNGRADE": Downgrade,
	"UPDOWN":    UpDown,
	"WRITE":     Write,
	"READ":      Read,
	"WRITEREAD": WriteRead,
}

// MagicByte is prepended to the serialized payload
const MagicByte byte = 0x0

// MessageFactory is a factory function, which should return a pointer to
// an instance into which we will unmarshal wire data.
// For Avro, the name will be the name of the Avro type if it has one.
// For JSON Schema, the name will be empty.
// For Protobuf, the name will be the name of the message type.
type MessageFactory func(subject string, name string) (interface{}, error)

// Serializer represents a serializer
type Serializer interface {
	ConfigureSerializer(client schemaregistry.Client, serdeType Type, conf *SerializerConfig) error
	// Serialize will serialize the given message, which should be a pointer.
	// For example, in Protobuf, messages are always a pointer to a struct and never just a struct.
	Serialize(topic string, msg interface{}) ([]byte, error)
	Close()
}

// Deserializer represents a deserializer
type Deserializer interface {
	ConfigureDeserializer(client schemaregistry.Client, serdeType Type, conf *DeserializerConfig) error
	// Deserialize will call the MessageFactory to create an object
	// into which we will unmarshal data.
	Deserialize(topic string, payload []byte) (interface{}, error)
	// DeserializeInto will unmarshal data into the given object.
	DeserializeInto(topic string, payload []byte, msg interface{}) error
	Close()
}

// Serde is a common instance for both the serializers and deserializers
type Serde struct {
	Client              schemaregistry.Client
	SerdeType           Type
	SubjectNameStrategy SubjectNameStrategyFunc
	MessageFactory      MessageFactory
}

// BaseSerializer represents basic serializer info
type BaseSerializer struct {
	Serde
	Conf *SerializerConfig
}

// BaseDeserializer represents basic deserializer info
type BaseDeserializer struct {
	Serde
	Conf *DeserializerConfig
}

// RuleContext represents a rule context
type RuleContext struct {
	Source        *schemaregistry.SchemaInfo
	Target        *schemaregistry.SchemaInfo
	Subject       string
	Topic         string
	IsKey         bool
	RuleMode      RuleMode
	Rule          *schemaregistry.Rule
	Index         int
	Rules         []schemaregistry.Rule
	fieldContexts []FieldContext
}

func (r *RuleContext) GetParameter(name string) *string {
	params := r.Rule.Params
	value, ok := params[name]
	if ok {
		return &value
	}
	metadata := r.Target.Metadata
	if metadata != nil {
		value, ok = metadata.Properties[name]
		if ok {
			return &value
		}
	}
	return nil
}

func (r *RuleContext) CurrentField() *FieldContext {
	size := len(r.fieldContexts)
	if size == 0 {
		return nil
	}
	return &r.fieldContexts[size-1]
}

func (r *RuleContext) EnterField(containingMessage interface{}, fullName string,
	name string, fieldType FieldType, tags []string) (FieldContext, bool) {
	allTags := make(map[string]bool)
	for _, v := range tags {
		allTags[v] = true
	}
	for k, v := range r.GetTags(fullName) {
		allTags[k] = v
	}
	fieldContext := FieldContext{
		ContainingMessage: containingMessage,
		FullName:          fullName,
		Name:              name,
		Type:              fieldType,
		Tags:              allTags,
	}
	r.fieldContexts = append(r.fieldContexts, fieldContext)
	return fieldContext, true
}

func (r *RuleContext) GetTags(fullName string) map[string]bool {
	tags := make(map[string]bool)
	metadata := r.Target.Metadata
	if metadata != nil && metadata.Tags != nil {
		for k, v := range metadata.Tags {
			if match(fullName, k) {
				for _, tag := range v {
					tags[tag] = true
				}
			}
		}
	}
	return tags
}

func (r *RuleContext) LeaveField() {
	size := len(r.fieldContexts) - 1
	r.fieldContexts = r.fieldContexts[:size]
}

// RuleBase represents a rule base
type RuleBase interface {
	Configure(clientConfig *schemaregistry.Config, config map[string]string) error
	Type() string
	Close()
}

// RuleExecutor represents a rule executor
type RuleExecutor interface {
	RuleBase
	Transform(ctx RuleContext, msg interface{}) (interface{}, error)
}

// FieldTransformer represents a field transformer
type FieldTransformer func(ctx RuleContext, fieldTransform FieldTransform, msg interface{}) (interface{}, error)

// FieldTransform represents a field transform
type FieldTransform interface {
	Transform(ctx RuleContext, fieldCtx FieldContext, fieldValue interface{}) (interface{}, error)
}

// FieldRuleExecutor represents a field rule executor
type FieldRuleExecutor interface {
	RuleExecutor
	SetFieldTransformer(transformer FieldTransformer)
	NewTransform(ctx RuleContext) (FieldTransform, error)
}

type AbstractFieldRuleExecutor struct {
	FieldRuleExecutor
	FieldTransformer FieldTransformer
}

func (a *AbstractFieldRuleExecutor) SetFieldTransformer(transformer FieldTransformer) {
	a.FieldTransformer = transformer
}

func (a *AbstractFieldRuleExecutor) Transform(ctx RuleContext, msg interface{}) (interface{}, error) {
	// TODO preserve source?
	switch ctx.RuleMode {
	case Write, Upgrade:
		for i := 0; i < ctx.Index; i++ {
			otherRule := ctx.Rules[i]
			if areTransformsWithSameTag(*ctx.Rule, otherRule) {
				// ignore this transform if an earlier one has the same tag
				return msg, nil
			}
		}
	case Read, Downgrade:
		for i := ctx.Index + 1; i < len(ctx.Rules); i++ {
			otherRule := ctx.Rules[i]
			if areTransformsWithSameTag(*ctx.Rule, otherRule) {
				// ignore this transform if a later one has the same tag
				return msg, nil
			}
		}
	}

	fieldTransform, err := a.NewTransform(ctx)
	if err != nil {
		return nil, err
	}
	// TODO preserve source?
	return a.FieldTransformer(ctx, fieldTransform, msg)
}

func areTransformsWithSameTag(rule1 schemaregistry.Rule, rule2 schemaregistry.Rule) bool {
	return len(rule1.Tags) > 0 && rule1.Kind == "TRANSFORM" && rule1.Kind == rule2.Kind && rule1.Mode == rule2.Mode &&
		rule1.Type == rule2.Type && reflect.DeepEqual(rule1.Tags, rule2.Tags)
}

// FieldContext represents a field context
type FieldContext struct {
	ContainingMessage interface{}
	FullName          string
	Name              string
	Type              FieldType
	Tags              map[string]bool
}

// FieldType represents the field type
type FieldType = int

const (
	// TypeRecord represents a record
	TypeRecord = 1
	// TypeEnum represents an enum
	TypeEnum = 2
	// TypeArray represents an array
	TypeArray = 3
	// TypeMap represents a map
	TypeMap = 4
	// TypeCombined represents a combined
	TypeCombined = 5
	// TypeFixed represents a fixed
	TypeFixed = 6
	// TypeString represents a string
	TypeString = 7
	// TypeBytes represents bytes
	TypeBytes = 8
	// TypeInt represents an int
	TypeInt = 9
	// TypeLong represents a long
	TypeLong = 10
	// TypeFloat represents a float
	TypeFloat = 11
	// TypeDouble represents a double
	TypeDouble = 12
	// TypeBoolean represents a Boolean
	TypeBoolean = 13
	// TypeNull represents a null
	TypeNull = 14
)

// RuleAction represents a rule action
type RuleAction interface {
	RuleBase
	Run(ctx RuleContext, msg interface{}, err error) error
}

// ErrorAction represents an error action
type ErrorAction struct {
}

// NoneAction represents a no-op action
type NoneAction struct {
}

type RuleConditionErr struct {
	Rule *schemaregistry.Rule
}

func (re RuleConditionErr) Error() string {
	errMsg := re.Rule.Doc
	if errMsg == "" {
		if re.Rule.Expr != "" {
			return "Expr failed: '" + re.Rule.Expr + "'"
		}
		return "Condition failed: '" + re.Rule.Name + "'"
	}
	return errMsg
}

// ConfigureSerializer configures the Serializer
func (s *BaseSerializer) ConfigureSerializer(client schemaregistry.Client, serdeType Type, conf *SerializerConfig) error {
	if client == nil {
		return fmt.Errorf("schema registry client missing")
	}
	s.Client = client
	s.Conf = conf
	s.SerdeType = serdeType
	s.SubjectNameStrategy = TopicNameStrategy
	return nil
}

// ConfigureDeserializer configures the Deserializer
func (s *BaseDeserializer) ConfigureDeserializer(client schemaregistry.Client, serdeType Type, conf *DeserializerConfig) error {
	if client == nil {
		return fmt.Errorf("schema registry client missing")
	}
	s.Client = client
	s.Conf = conf
	s.SerdeType = serdeType
	s.SubjectNameStrategy = TopicNameStrategy
	return nil
}

// SubjectNameStrategyFunc determines the subject for the given parameters
type SubjectNameStrategyFunc func(topic string, serdeType Type, schema schemaregistry.SchemaInfo) (string, error)

// TopicNameStrategy creates a subject name by appending -[key|value] to the topic name.
func TopicNameStrategy(topic string, serdeType Type, schema schemaregistry.SchemaInfo) (string, error) {
	suffix := "-value"
	if serdeType == KeySerde {
		suffix = "-key"
	}
	return topic + suffix, nil
}

// GetID returns a schema ID for the given schema
func (s *BaseSerializer) GetID(topic string, msg interface{}, info *schemaregistry.SchemaInfo) (int, error) {
	autoRegister := s.Conf.AutoRegisterSchemas
	useSchemaID := s.Conf.UseSchemaID
	useLatest := s.Conf.UseLatestVersion
	normalizeSchema := s.Conf.NormalizeSchemas

	var id = -1
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, *info)
	if err != nil {
		return -1, err
	}
	if autoRegister {
		id, err = s.Client.Register(subject, *info, normalizeSchema)
		if err != nil {
			return -1, err
		}
	} else if useSchemaID >= 0 {
		*info, err = s.Client.GetBySubjectAndID(subject, useSchemaID)
		if err != nil {
			return -1, err
		}
		id, err = s.Client.GetID(subject, *info, false)
		if err != nil {
			return -1, err
		}
		if id != useSchemaID {
			return -1, fmt.Errorf("failed to match schema ID (%d != %d)", id, useSchemaID)
		}
	} else if useLatest {
		metadata, err := s.Client.GetLatestSchemaMetadata(subject)
		if err != nil {
			return -1, err
		}
		*info = schemaregistry.SchemaInfo{
			Schema:     metadata.Schema,
			SchemaType: metadata.SchemaType,
			References: metadata.References,
			Metadata:   metadata.Metadata,
			Ruleset:    metadata.Ruleset,
		}
		id, err = s.Client.GetID(subject, *info, false)
		if err != nil {
			return -1, err
		}
	} else {
		id, err = s.Client.GetID(subject, *info, normalizeSchema)
		if err != nil {
			return -1, err
		}
	}
	return id, nil
}

func (s *Serde) ExecuteRules(subject string, topic string, ruleMode RuleMode,
	source *schemaregistry.SchemaInfo, target *schemaregistry.SchemaInfo, msg interface{}) (interface{}, error) {
	if msg == nil || target == nil {
		return msg, nil
	}
	var rules []schemaregistry.Rule
	switch ruleMode {
	case Upgrade:
		if target.Ruleset != nil {
			rules = target.Ruleset.MigrationRules
		}
	case Downgrade:
		if source.Ruleset != nil {
			// Execute downgrade rules in reverse order for symmetry
			rules = reverseRules(source.Ruleset.MigrationRules)
		}
	default:
		if target.Ruleset != nil {
			rules = target.Ruleset.DomainRules
			if ruleMode == Read {
				// Execute read rules in reverse order for symmetry
				rules = reverseRules(rules)
			}
		}
	}
	for i, rule := range rules {
		if rule.Disabled {
			continue
		}
		mode, ok := parseMode(rule.Mode)
		if !ok {
			continue
		}
		switch mode {
		case WriteRead:
			if ruleMode != Write && ruleMode != Read {
				continue
			}
		case UpDown:
			if ruleMode != Upgrade && ruleMode != Downgrade {
				continue
			}
		default:
			if mode != ruleMode {
				continue
			}
		}
		ctx := RuleContext{
			Source:   source,
			Target:   target,
			Subject:  subject,
			Topic:    topic,
			IsKey:    s.SerdeType == KeySerde,
			RuleMode: ruleMode,
			Rule:     &rule,
			Index:    i,
			Rules:    rules,
		}
		ruleExecutor := GetRuleExecutor(rule.Type)
		if ruleExecutor == nil {
			err := s.runAction(ctx, ruleMode, rule, rule.OnFailure, msg,
				fmt.Errorf("could not find rule executor of type %s", rule.Type), "ERROR")
			if err != nil {
				return nil, err
			}
			return msg, nil
		}
		var err error
		msg, err = ruleExecutor.Transform(ctx, msg)
		if err != nil {
			err = s.runAction(ctx, ruleMode, rule, rule.OnFailure, msg, err, "ERROR")
			if err != nil {
				return nil, err
			}
		} else if msg == nil {
			err = s.runAction(ctx, ruleMode, rule, rule.OnFailure, msg, nil, "ERROR")
			if err != nil {
				return nil, err
			}
		} else {
			err = s.runAction(ctx, ruleMode, rule, rule.OnSuccess, msg, nil, "NONE")
			if err != nil {
				return nil, err
			}
		}
	}
	return msg, nil
}

func parseMode(mode string) (RuleMode, bool) {
	c, ok := modes[strings.ToUpper(mode)]
	return c, ok
}

func reverseRules(rules []schemaregistry.Rule) []schemaregistry.Rule {
	newRules := make([]schemaregistry.Rule, len(rules))
	copy(newRules, rules)
	// Execute downgrade rules in reverse order for symmetry
	for i, j := 0, len(newRules)-1; i < j; i, j = i+1, j-1 {
		newRules[i], newRules[j] = newRules[j], newRules[i]
	}
	return newRules
}

func (s *Serde) runAction(ctx RuleContext, ruleMode RuleMode, rule schemaregistry.Rule,
	action string, msg interface{}, err error, defaultAction string) error {
	actionName := s.getRuleActionName(rule, ruleMode, action)
	if actionName == nil {
		actionName = &defaultAction
	}
	ruleAction := s.getRuleAction(ctx, *actionName)
	if ruleAction == nil {
		log.Printf("could not find rule action of type %s", *actionName)
		return fmt.Errorf("could not find rule action of type %s", *actionName)
	}
	e := ruleAction.Run(ctx, msg, err)
	if e != nil {
		log.Printf("WARN: could not run post-rule action %s, error: %v", *actionName, e)
		return e
	}
	return nil
}

func (s *Serde) getRuleActionName(rule schemaregistry.Rule, ruleMode RuleMode, actionName string) *string {
	if actionName == "" {
		return nil
	}
	mode, ok := parseMode(rule.Mode)
	if !ok {
		return nil
	}
	if (mode == WriteRead || mode == UpDown) && strings.Contains(actionName, ",") {
		parts := strings.Split(actionName, ",")
		switch ruleMode {
		case Write, Upgrade:
			return &parts[0]
		case Read, Downgrade:
			return &parts[1]
		default:
			return nil
		}
	}
	return &actionName
}

func (s *Serde) getRuleAction(ctx RuleContext, actionName string) RuleAction {
	if actionName == "ERROR" {
		return ErrorAction{}
	} else if actionName == "NONE" {
		return NoneAction{}
	} else {
		return GetRuleAction(actionName)
	}
}

// WriteBytes writes the serialized payload prepended by the MagicByte
func (s *BaseSerializer) WriteBytes(id int, msgBytes []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := buf.WriteByte(MagicByte)
	if err != nil {
		return nil, err
	}
	idBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(idBytes, uint32(id))
	_, err = buf.Write(idBytes)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(msgBytes)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GetSchema returns a schema for a payload
func (s *BaseDeserializer) GetSchema(topic string, payload []byte) (schemaregistry.SchemaInfo, error) {
	info := schemaregistry.SchemaInfo{}
	if payload[0] != MagicByte {
		return info, fmt.Errorf("unknown magic byte")
	}
	id := binary.BigEndian.Uint32(payload[1:5])
	subject, err := s.SubjectNameStrategy(topic, s.SerdeType, info)
	if err != nil {
		return info, err
	}
	return s.Client.GetBySubjectAndID(subject, int(id))
}

// ResolveReferences resolves schema references
func ResolveReferences(c schemaregistry.Client, schema schemaregistry.SchemaInfo, deps map[string]string) error {
	for _, ref := range schema.References {
		metadata, err := c.GetSchemaMetadata(ref.Subject, ref.Version)
		if err != nil {
			return err
		}
		info := schemaregistry.SchemaInfo{
			Schema:     metadata.Schema,
			SchemaType: metadata.SchemaType,
			References: metadata.References,
			Metadata:   metadata.Metadata,
			Ruleset:    metadata.Ruleset,
		}
		deps[ref.Name] = metadata.Schema
		err = ResolveReferences(c, info, deps)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the Serde
func (s *Serde) Close() {
}

// Configure configures the action
func (a ErrorAction) Configure(clientConfig *schemaregistry.Config, config map[string]string) error {
	return nil
}

// Type returns the type
func (a ErrorAction) Type() string {
	return "ERROR"
}

// Run runs the action
func (a ErrorAction) Run(ctx RuleContext, msg interface{}, err error) error {
	return fmt.Errorf("rule failed: %s, error: %v", ctx.Rule.Name, err)
}

// Close closes the action
func (a ErrorAction) Close() {
}

// Configure configures the action
func (a NoneAction) Configure(clientConfig *schemaregistry.Config, config map[string]string) error {
	return nil
}

// Type returns the type
func (a NoneAction) Type() string {
	return "NONE"
}

// Run runs the action
func (a NoneAction) Run(ctx RuleContext, msg interface{}, err error) error {
	return nil
}

// Close closes the action
func (a NoneAction) Close() {
}
