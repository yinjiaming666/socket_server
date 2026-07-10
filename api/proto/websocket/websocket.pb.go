// Code generated manually to match api/proto/websocket.proto. DO NOT EDIT.
package websocketpb

import (
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/descriptorpb"
)

// PushMessage 表示内部推送消息。
type PushMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MessageId   string   `protobuf:"bytes,1,opt,name=message_id,json=messageId,proto3" json:"message_id,omitempty"`
	FromUserId  string   `protobuf:"bytes,2,opt,name=from_user_id,json=fromUserId,proto3" json:"from_user_id,omitempty"`
	BizType     string   `protobuf:"bytes,3,opt,name=biz_type,json=bizType,proto3" json:"biz_type,omitempty"`
	Payload     []byte   `protobuf:"bytes,4,opt,name=payload,proto3" json:"payload,omitempty"`
	AckRequired bool     `protobuf:"varint,5,opt,name=ack_required,json=ackRequired,proto3" json:"ack_required,omitempty"`
	CreatedAt   int64    `protobuf:"varint,6,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	ToUserIds   []string `protobuf:"bytes,7,rep,name=to_user_ids,json=toUserIds,proto3" json:"to_user_ids,omitempty"`
	ConnIds     []string `protobuf:"bytes,8,rep,name=conn_ids,json=connIds,proto3" json:"conn_ids,omitempty"`
	GroupId     string   `protobuf:"bytes,9,opt,name=group_id,json=groupId,proto3" json:"group_id,omitempty"`
	RoomId      string   `protobuf:"bytes,10,opt,name=room_id,json=roomId,proto3" json:"room_id,omitempty"`
}

func (x *PushMessage) Reset() {
	*x = PushMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *PushMessage) String() string { return protoimpl.X.MessageStringOf(x) }
func (*PushMessage) ProtoMessage()    {}
func (x *PushMessage) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *PushMessage) GetMessageId() string {
	if x != nil {
		return x.MessageId
	}
	return ""
}
func (x *PushMessage) GetFromUserId() string {
	if x != nil {
		return x.FromUserId
	}
	return ""
}
func (x *PushMessage) GetBizType() string {
	if x != nil {
		return x.BizType
	}
	return ""
}
func (x *PushMessage) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}
func (x *PushMessage) GetAckRequired() bool {
	if x != nil {
		return x.AckRequired
	}
	return false
}
func (x *PushMessage) GetCreatedAt() int64 {
	if x != nil {
		return x.CreatedAt
	}
	return 0
}
func (x *PushMessage) GetToUserIds() []string {
	if x != nil {
		return x.ToUserIds
	}
	return nil
}
func (x *PushMessage) GetConnIds() []string {
	if x != nil {
		return x.ConnIds
	}
	return nil
}
func (x *PushMessage) GetGroupId() string {
	if x != nil {
		return x.GroupId
	}
	return ""
}
func (x *PushMessage) GetRoomId() string {
	if x != nil {
		return x.RoomId
	}
	return ""
}

// PushToConnsRequest 表示按连接推送请求。
type PushToConnsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ConnIds []string     `protobuf:"bytes,1,rep,name=conn_ids,json=connIds,proto3" json:"conn_ids,omitempty"`
	Message *PushMessage `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *PushToConnsRequest) Reset() {
	*x = PushToConnsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *PushToConnsRequest) String() string { return protoimpl.X.MessageStringOf(x) }
func (*PushToConnsRequest) ProtoMessage()    {}
func (x *PushToConnsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *PushToConnsRequest) GetConnIds() []string {
	if x != nil {
		return x.ConnIds
	}
	return nil
}
func (x *PushToConnsRequest) GetMessage() *PushMessage {
	if x != nil {
		return x.Message
	}
	return nil
}

// PushToUsersRequest 表示按用户推送请求。
type PushToUsersRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	UserIds []string     `protobuf:"bytes,1,rep,name=user_ids,json=userIds,proto3" json:"user_ids,omitempty"`
	Message *PushMessage `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *PushToUsersRequest) Reset() {
	*x = PushToUsersRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *PushToUsersRequest) String() string { return protoimpl.X.MessageStringOf(x) }
func (*PushToUsersRequest) ProtoMessage()    {}
func (x *PushToUsersRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *PushToUsersRequest) GetUserIds() []string {
	if x != nil {
		return x.UserIds
	}
	return nil
}
func (x *PushToUsersRequest) GetMessage() *PushMessage {
	if x != nil {
		return x.Message
	}
	return nil
}

// PushResponse 表示节点推送响应。
type PushResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Accepted        bool     `protobuf:"varint,1,opt,name=accepted,proto3" json:"accepted,omitempty"`
	AcceptedConnIds []string `protobuf:"bytes,2,rep,name=accepted_conn_ids,json=acceptedConnIds,proto3" json:"accepted_conn_ids,omitempty"`
	FailedConnIds   []string `protobuf:"bytes,3,rep,name=failed_conn_ids,json=failedConnIds,proto3" json:"failed_conn_ids,omitempty"`
	Reason          string   `protobuf:"bytes,4,opt,name=reason,proto3" json:"reason,omitempty"`
}

func (x *PushResponse) Reset() {
	*x = PushResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *PushResponse) String() string { return protoimpl.X.MessageStringOf(x) }
func (*PushResponse) ProtoMessage()    {}
func (x *PushResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *PushResponse) GetAccepted() bool {
	if x != nil {
		return x.Accepted
	}
	return false
}
func (x *PushResponse) GetAcceptedConnIds() []string {
	if x != nil {
		return x.AcceptedConnIds
	}
	return nil
}
func (x *PushResponse) GetFailedConnIds() []string {
	if x != nil {
		return x.FailedConnIds
	}
	return nil
}
func (x *PushResponse) GetReason() string {
	if x != nil {
		return x.Reason
	}
	return ""
}

// HealthRequest 表示节点健康检查请求。
type HealthRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EndpointId string `protobuf:"bytes,1,opt,name=endpoint_id,json=endpointId,proto3" json:"endpoint_id,omitempty"`
}

func (x *HealthRequest) Reset() {
	*x = HealthRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *HealthRequest) String() string { return protoimpl.X.MessageStringOf(x) }
func (*HealthRequest) ProtoMessage()    {}
func (x *HealthRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *HealthRequest) GetEndpointId() string {
	if x != nil {
		return x.EndpointId
	}
	return ""
}

// HealthResponse 表示节点健康检查响应。
type HealthResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EndpointId string `protobuf:"bytes,1,opt,name=endpoint_id,json=endpointId,proto3" json:"endpoint_id,omitempty"`
	Status     string `protobuf:"bytes,2,opt,name=status,proto3" json:"status,omitempty"`
	ConnCount  int64  `protobuf:"varint,3,opt,name=conn_count,json=connCount,proto3" json:"conn_count,omitempty"`
}

func (x *HealthResponse) Reset() {
	*x = HealthResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *HealthResponse) String() string { return protoimpl.X.MessageStringOf(x) }
func (*HealthResponse) ProtoMessage()    {}
func (x *HealthResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *HealthResponse) GetEndpointId() string {
	if x != nil {
		return x.EndpointId
	}
	return ""
}
func (x *HealthResponse) GetStatus() string {
	if x != nil {
		return x.Status
	}
	return ""
}
func (x *HealthResponse) GetConnCount() int64 {
	if x != nil {
		return x.ConnCount
	}
	return 0
}

var File_api_proto_websocket_proto protoreflect.FileDescriptor
var file_api_proto_websocket_proto_once sync.Once
var file_api_proto_websocket_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_api_proto_websocket_proto_goTypes = []any{(*PushMessage)(nil), (*PushToConnsRequest)(nil), (*PushToUsersRequest)(nil), (*PushResponse)(nil), (*HealthRequest)(nil), (*HealthResponse)(nil)}
var file_api_proto_websocket_proto_depIdxs = []int32{0, 0, 1, 3, 2, 3, 4, 5}

func init() { file_api_proto_websocket_proto_init() }

func file_api_proto_websocket_proto_init() {
	file_api_proto_websocket_proto_once.Do(func() {
		if File_api_proto_websocket_proto != nil {
			return
		}
		if !protoimpl.UnsafeEnabled {
			file_api_proto_websocket_proto_msgTypes[0].Exporter = func(v any, i int) any {
				switch v := v.(*PushMessage); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[1].Exporter = func(v any, i int) any {
				switch v := v.(*PushToConnsRequest); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[2].Exporter = func(v any, i int) any {
				switch v := v.(*PushToUsersRequest); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[3].Exporter = func(v any, i int) any {
				switch v := v.(*PushResponse); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[4].Exporter = func(v any, i int) any {
				switch v := v.(*HealthRequest); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[5].Exporter = func(v any, i int) any {
				switch v := v.(*HealthResponse); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
		}
		type x struct{}
		out := protoimpl.TypeBuilder{
			File:              protoimpl.DescBuilder{GoPackagePath: reflect.TypeOf(x{}).PkgPath(), RawDescriptor: websocketRawDescriptor(), NumEnums: 0, NumMessages: 6, NumExtensions: 0, NumServices: 1},
			GoTypes:           file_api_proto_websocket_proto_goTypes,
			DependencyIndexes: file_api_proto_websocket_proto_depIdxs,
			MessageInfos:      file_api_proto_websocket_proto_msgTypes,
		}.Build()
		File_api_proto_websocket_proto = out.File
	})
}

func websocketRawDescriptor() []byte {
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	tString := descriptorpb.FieldDescriptorProto_TYPE_STRING
	tBytes := descriptorpb.FieldDescriptorProto_TYPE_BYTES
	tBool := descriptorpb.FieldDescriptorProto_TYPE_BOOL
	tInt64 := descriptorpb.FieldDescriptorProto_TYPE_INT64
	tMessage := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("api/proto/websocket.proto"),
		Package: proto.String("websocket.v1"),
		Syntax:  proto.String("proto3"),
		Options: &descriptorpb.FileOptions{GoPackage: proto.String("socket_server/api/proto/websocket;websocketpb")},
		MessageType: []*descriptorpb.DescriptorProto{
			msg("PushMessage", field("message_id", 1, optional, tString, ""), field("from_user_id", 2, optional, tString, ""), field("biz_type", 3, optional, tString, ""), field("payload", 4, optional, tBytes, ""), field("ack_required", 5, optional, tBool, ""), field("created_at", 6, optional, tInt64, ""), field("to_user_ids", 7, repeated, tString, ""), field("conn_ids", 8, repeated, tString, ""), field("group_id", 9, optional, tString, ""), field("room_id", 10, optional, tString, "")),
			msg("PushToConnsRequest", field("conn_ids", 1, repeated, tString, ""), field("message", 2, optional, tMessage, ".websocket.v1.PushMessage")),
			msg("PushToUsersRequest", field("user_ids", 1, repeated, tString, ""), field("message", 2, optional, tMessage, ".websocket.v1.PushMessage")),
			msg("PushResponse", field("accepted", 1, optional, tBool, ""), field("accepted_conn_ids", 2, repeated, tString, ""), field("failed_conn_ids", 3, repeated, tString, ""), field("reason", 4, optional, tString, "")),
			msg("HealthRequest", field("endpoint_id", 1, optional, tString, "")),
			msg("HealthResponse", field("endpoint_id", 1, optional, tString, ""), field("status", 2, optional, tString, ""), field("conn_count", 3, optional, tInt64, "")),
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("WebSocketPushService"),
			Method: []*descriptorpb.MethodDescriptorProto{
				{Name: proto.String("PushToConns"), InputType: proto.String(".websocket.v1.PushToConnsRequest"), OutputType: proto.String(".websocket.v1.PushResponse")},
				{Name: proto.String("PushToUsers"), InputType: proto.String(".websocket.v1.PushToUsersRequest"), OutputType: proto.String(".websocket.v1.PushResponse")},
				{Name: proto.String("Health"), InputType: proto.String(".websocket.v1.HealthRequest"), OutputType: proto.String(".websocket.v1.HealthResponse")},
			},
		}},
	}
	raw, _ := proto.Marshal(fd)
	return raw
}

func msg(name string, fields ...*descriptorpb.FieldDescriptorProto) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{Name: proto.String(name), Field: fields}
}
func field(name string, number int32, label descriptorpb.FieldDescriptorProto_Label, typ descriptorpb.FieldDescriptorProto_Type, typeName string) *descriptorpb.FieldDescriptorProto {
	f := &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Label: &label, Type: &typ}
	if typeName != "" {
		f.TypeName = proto.String(typeName)
	}
	return f
}
