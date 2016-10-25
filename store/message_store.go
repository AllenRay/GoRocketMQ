package store

type PutMessageResult struct {
	PutMessageStatus   int32
	AppendMessageResut AppendMessageResut
}

type MessageStore interface {
	PutMessage(message *Message) (PutMessageResult, bool)
}
