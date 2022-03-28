// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"lspnet"
	"strconv"
	"strings"
	"time"
)

type client struct {
	// TODO: implement this!
	CID int
	Seqnum int
	Closed chan int
	Params *Params
	Conn *lspnet.UDPConn
	UDPADDr *lspnet.UDPAddr
	RecvWin chan *Message
	SndBuffer chan *Message // buff of msg sent
	SndWin chan *Message //msg to be sent immediately , size restricted
	SliBuf map[int]*Message // Maintain Buff to be resent
	pointer int
	SlideWin map[int]*Message
	SlideMin int
	is_closed int
	SndStat chan int //send status
	Snd_Empty chan int //sndwin empty flag
	last_seq_num int //last sequence num
	done_flag chan int //whether send is end or not
	done_flag_cli chan int //client close
	closed_all chan int
	epochtimer *time.Timer
	exceed_time int
	block_update int
	done_close chan int
	close_converge chan int
	Data_Received chan *Message
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	rAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, rAddr)
	if err != nil {
		return nil, err
	}
	c := createClient(params, conn, rAddr)
	c.epochtimer.Reset(time.Millisecond * time.Duration(c.Params.EpochMillis))
	newConn := NewConnect()
	clientSendMessage(c, newConn)
	readBytes := make([]byte, MAXN)
	for {
		select {
		case <-c.epochtimer.C:
			//fmt.Println("Client epoch time in connection.")
			c.exceed_time++

			//fmt.Println("Client:"+strconv.Itoa(c.CID)+" retrying to connect")
			if c.exceed_time >= c.Params.EpochLimit {
				return nil, errors.New("Error Connection not established.")
			}
			clientSendMessage(c, newConn)
			c.epochtimer.Reset(time.Millisecond * time.Duration(params.EpochMillis))
		default:
			msg, rAddr, err := c.clientRecvMessage(readBytes)
			if err != nil {
				continue
			}
			c.exceed_time = 0
			if strings.EqualFold(rAddr.String(), c.UDPADDr.String()) {
				if msg.Type == MsgAck && msg.SeqNum == -1 {
					c.CID= msg.ConnID
					fmt.Println("Connection"+strconv.Itoa(c.CID)+" established.")
					go CliMsgMnt(c,clientSendMessage)
					go CliRecvRout(c)
					return c, nil
				}
			}
			return nil,errors.New("Error occurs in establishing connections")
		}
	}
}

func (c *client) ConnID() int {
	return c.CID
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {
	case msg, ok := <-c.Data_Received:
		if !ok || c.is_closed==1{
			return nil, errors.New("Read error, the server has been lost.")
		}
		return msg.Payload, nil
	}
}

func (c *client) Write(payload []byte) error {
	if(c.is_closed==1){
		return errors.New("connection has been closed")
	}
	//fmt.Println("Client Trying to Write:"+string(payload[:]))

	msg := NewData(c.CID, c.Seqnum, payload)
	c.Seqnum++
	c.SndBuffer <- msg
	select{
	case <-c.done_flag:
		<-c.done_flag_cli
		c.block_update=0
		c.SndStat<-1
		break
	default:
		break
	}

	//fmt.Println("Client Write:" + msg.String(), time.Now())
	return nil
}

func (c *client) Close() error {
	if(c.is_closed==1){
		println("server has been closed")
		return errors.New("server has been closed")
	}
	c.is_closed=1
	<-c.done_flag_cli
	select{
	case c.Closed<-1:
		//println("c.closed set to 1")
		break
	default:

		return errors.New("Server has been closed")
	}
	<-c.closed_all
	c.Conn.Close()
	c.close_converge<-1
	fmt.Println("Client:"+strconv.Itoa(c.CID)+" closed")
	return nil
}

func createClient(params *Params, conn *lspnet.UDPConn, rAddr *lspnet.UDPAddr) *client {
	c:=&client{
		Params:    params,
		Conn:      conn,
		UDPADDr:   rAddr,
		epochtimer:        time.NewTimer(0),
		Closed:    make(chan int, 1), //may get stuck sometimes , no more than 2 closed op awaiting
		RecvWin:   make(chan *Message, MAXN),
		SndBuffer: make(chan *Message, MAXN),
		Data_Received: make(chan *Message,MAXN),
		last_seq_num: 0,
		SndWin:    make(chan *Message, params.WindowSize),
		SliBuf:    make(map[int]*Message, params.WindowSize),
		SlideWin:  make(map[int]*Message, params.WindowSize),
		SndStat:   make(chan int, 1),
		Snd_Empty: make(chan int, 1),
		done_flag: make(chan int, 1),
		done_flag_cli: make(chan int, 1),
		done_close: make(chan int ,1),
		closed_all: make(chan int ,1),
		close_converge:make(chan int,1), //for server usage
	}
	c.done_flag<-1
	c.done_flag_cli<-1
	return c
}
func CliRecvRout(c *client){
	readBytes := make([]byte, MAXN)
	for {
		select {
		case <-c.done_close:
			close(c.RecvWin)
			println("Client Side closed")
			return
		default:
			msg, _, err := c.clientRecvMessage(readBytes)

			if err != nil {
				continue
			}
			c.RecvWin<-msg
		}
	}
}
func CliMsgMnt(c *client,sendMessage func(*client, *Message) error){
	select{
	case <-c.epochtimer.C:
		break
	default:
		break
	}
	for{
		select{
		case <-c.Closed://optional : wait until all events to be done
			c.done_close<-1
			c.closed_all<-1
			fmt.Println("Client Closed")
			return
		case msg:=<-c.RecvWin:
			//println(msg.String())
			if(msg.Type == MsgData){
				//fmt.Println("data recv:" + msg.String(), time.Now())
				c.Data_Received<-msg //need revise , duplicated packets
				ack:=NewAck(c.CID,msg.SeqNum)
				sendMessage(c,ack)
			}
			if(msg.Type == MsgAck){
				//fmt.Println("ack recv:" + msg.String(), time.Now())
				if(msg.SeqNum >= c.SlideMin && msg.SeqNum < c.SlideMin+c.Params.WindowSize){
					//fmt.Println("Msg in the slideWin,seqnum:"+strconv.Itoa(msg.SeqNum))
					_,ok :=c.SlideWin[msg.SeqNum]
					if !ok {
						c.SlideWin[msg.SeqNum] = msg
					}
					//update Slide Window
					_,ok = c.SlideWin[c.SlideMin]
					if ok{
						//fmt.Println("reseting timer and update")
						c.epochtimer.Reset(time.Millisecond * time.Duration(c.Params.EpochMillis))
						delete(c.SlideWin,c.SlideMin)
						p:=1
						for i:=1;i<c.Params.WindowSize;i++{
							_,ok = c.SlideWin[c.SlideMin+i]
							if ok{
								delete(c.SlideWin,c.SlideMin+i)
								p++
							}else{
								break
							}
						}
						SndMsgUpdate(c,p)
					}
					//
					//proceed last packages
					select{
					case <-c.Snd_Empty:
						//fmt.Println("Processing Empty case,slidemin:"+strconv.Itoa(c.SlideMin)+",last_seq:"+strconv.Itoa(c.last_seq_num))
						if(c.SlideMin>c.last_seq_num && c.block_update==0){
							//print("All messages ackced,timer stoped\n")
							//fmt.Printf("last seq num is :%d\n",c.last_seq_num)
							c.done_flag<-1 //could be used in write procedure
							select {
							case c.done_flag_cli<-1:
								break
							default:
								break
							}
							c.block_update=1
							//print("Nonblocking\n")
							select{
							case <-c.epochtimer.C:
								break
							default:
								break
							}
							c.epochtimer.Stop()
							//c.epochtimer.Reset(time.Millisecond * time.Duration(c.Params.EpochMillis))
						}else{
							c.Snd_Empty<-1
						}
						//println("proc done")
					default:
						break
					}
				}
			}
		case msg:=<-c.SndWin:
			//msg to be sent immediately
			sendMessage(c,msg)
		case <-c.SndStat://first time msg sent,all previous msg must be acked
			//fmt.Println("sndstat event")
			select {
			case msg:=<-c.SndBuffer:
				//fmt.Println("SndBuffer load:"+msg.String())
				c.epochtimer.Reset(time.Millisecond * time.Duration(c.Params.EpochMillis))
				c.SlideMin=msg.SeqNum
				c.SliBuf[msg.SeqNum]=msg
				c.SndWin<-msg
				for i:=1;i<c.Params.WindowSize;i++{
					select {
					case tem:=<-c.SndBuffer:
						c.SliBuf[msg.SeqNum+i]=tem
						c.SndWin<-tem
					default:
						//fmt.Print("Message to be sent is too short\n")
						break
						}
					}
				break
			default:
				//fmt.Print("Start Error:No msg in buffer\n")
				continue
			}
		case <-c.epochtimer.C: //timed out
			c.exceed_time++
			//println("clock!"+strconv.Itoa(c.exceed_time))
			if(c.exceed_time >= c.Params.EpochLimit){
				fmt.Printf("Client:%d time exceeed,take abortion\n",c.CID)
				go c.Close()
				break
			}
			//resend
			c.epochtimer.Reset(time.Millisecond * time.Duration(c.Params.EpochMillis))
			if(c.block_update==1){
				break
			}
			//fmt.Printf("Client:%d time exceed,resend msgs\n",c.CID)

			if(len(c.SliBuf)==0){
				println("null")
				break
			}
			for _,v := range c.SliBuf{
				//fmt.Println("msg:"+v.String()+"resend")
				sendMessage(c,v)
			}
		}
	}
}

func SndMsgUpdate(c *client,p int) {
	tmp:=c.SlideMin
	for i := 0; i < p; i++ {
		c.SlideMin++
		select {
		case msg := <-c.SndBuffer: //assume that sndbuffer must be increment
			_,ok:=c.SliBuf[tmp+i]
			if ok{
				delete(c.SliBuf,tmp+i)
			}
			c.SliBuf[msg.SeqNum]=msg
			c.last_seq_num=msg.SeqNum
			//fmt.Println("SndMsgUpdate: seqnum:"+strconv.Itoa(msg.SeqNum))
			c.SndWin <- msg
			break
		default:
			//fmt.Println("SndBuffer Is Empty Now")
			select {
				case c.Snd_Empty<-1:
					break
				default:
				//fmt.Println("Err")
			}
			return

		}

	}

}

func clientSendMessage(c *client, msg *Message) error {
	writeBytes, err := json.Marshal(msg)
	if err != nil {
		//fmt.Println("Error client json.Marshal.", err.Error())
		return err
	}
	_, err = c.Conn.Write(writeBytes)
	if err != nil {
		//fmt.Println("Error client WriteToUDP.", err.Error())
		return err
	}
	//fmt.Println("Client send:"+msg.String(), time.Now())
	return nil
}

func (c *client) clientRecvMessage(readBytes []byte) (*Message, *lspnet.UDPAddr, error) {
	c.Conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(c.Params.EpochMillis)))
	readSize, rAddr, err := c.Conn.ReadFromUDP(readBytes)
	if err != nil {
		//fmt.Println("Error client ReadFromUDP.", err.Error())
		return nil, nil, err
	}
	var msg Message
	err = json.Unmarshal(readBytes[:readSize], &msg)
	if err != nil {
		//fmt.Println("Error json.Unmarshal.", err.Error())
		return nil, nil, err
	}
	//fmt.Println("Client recv:"+msg.String(), time.Now())
	return &msg, rAddr, nil
}
