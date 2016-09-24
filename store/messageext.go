package store

type Message struct {
	topic                    string
	flag                     int32
	properties               map[string]string
	body                     []byte
	queueId                  int32
	storeSize                int32
	queueOffset              int64
	sysFlag                  int32
	bordTimeStamp            int64
	host                     string
	storeTimeStamp           int64
	msgId                    string
	bodyCRC                  int32
	reConsumedTimes          int32
	prepareTaansactionOffset int64
}
