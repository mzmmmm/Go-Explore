// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"lspnet"
	"strconv"
	"sync"
	"time"
)
const MAXN=1000

type server struct {
	Clis map[int]*client
	Conn_Cnt int
	Seqs []int
	ConnMap map[string]int
	close_converge chan int
	close_read chan int
	LiveMsg chan *Message
	mutex sync.Mutex
	Closed chan int
	is_closed bool
	Params *Params
	Conn *lspnet.UDPConn
	// TODO: implement this!
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		//fmt.Println("Error ResolveUDPAddr.", err.Error())
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		//fmt.Println("Error ListenUDP.", err.Error())
		return nil, err
	}
	s := &server{
		Clis: make(map[int]*client),
		ConnMap: make(map[string]int),
		LiveMsg: make(chan *Message,MAXN),
		Closed:make(chan int,1),
		close_converge: make(chan int,1),
		close_read: make(chan int,1),
		Params: params,
		Conn:conn,
	}
	fmt.Println("Server listening on port:"+strconv.Itoa(port))
	go s.recvMsgLoop()
	return s,nil
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	for{
		select {
		case data := <- s.LiveMsg:
			s.mutex.Lock()
			_,ok:=s.Clis[data.ConnID]
			s.mutex.Unlock()
			if !ok{
				continue
			}  else{
				//fmt.Println("server read:"+data.String())
				return data.ConnID,data.Payload,nil
			}
		case <-s.close_read:
			return 0,nil,errors.New("server has been shutdown")
		}
	}
}

func (s *server) Write(connID int, payload []byte) error {

	s.mutex.Lock()
	c, ok := s.Clis[connID]
	s.mutex.Unlock()
	if !ok {
		return errors.New("The connection with client has closed.")
	}
	//fmt.Println("Server write Msg:"+string(payload[:]))
	msg := NewData(connID, c.Seqnum, payload)
	c.Seqnum++
	c.SndBuffer <- msg
	select{
	case <-c.done_flag:
		c.block_update=0
		c.SndStat<-1
		break
	default:
		break
	}
	return nil
}

func (s *server) CloseConn(connID int) error {
	s.mutex.Lock()
	c, ok := s.Clis[connID]
	s.mutex.Unlock()
	if !ok {
		return errors.New("The connecion with client has lost.")
	}

	c.Close()
	delete(s.Clis,connID)
	delete(s.ConnMap,c.UDPADDr.String())
	return nil
}

func (s *server) Close() error {
	println("Server closing")
	for _, c := range s.Clis {
		c.Close()
	}
	s.close_read<-1
	s.Closed<-1
	//need wait until all things done
	println("Server has been closed")
	s.Conn.Close()
	return nil
}

func Recv_Data_Converge(s *server, c *client) error{
	for{
		select{
			case msg:=<-c.Data_Received:
				//println("msg")
				s.LiveMsg<-msg
				break
			case <-s.close_converge:
				println("Converge closed")
				return errors.New("Converge closed")
			case <-c.close_converge :
				println("Converge closed")
				return errors.New("Converge closed")
		}
	}
}



func (s *server) recvMsgLoop() error{
	for{
		select{
		case <-s.Closed:
			s.close_converge<-1
			return errors.New("Server Closed")
		default:
			bytes:=make([]byte,MAXN)
			s.Conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(s.Params.EpochMillis)))
			msg, rAddr, err := s.serverRecvMessage(bytes)
			if err != nil {
				continue
			}
			if(msg.Type == MsgConnect){
				if(s.is_closed){
					continue
				}
				s.mutex.Lock()
				saddr:=rAddr.String()
				conn,ok:=s.ConnMap[saddr]
				if !ok{

					conn=s.Conn_Cnt
					s.ConnMap[saddr]=s.Conn_Cnt
					c:=createClient(s.Params,s.Conn,rAddr)
					s.Clis[s.Conn_Cnt]=c
					s.Clis[s.Conn_Cnt].CID=s.Conn_Cnt
					s.Conn_Cnt++
					c.epochtimer.Reset(time.Millisecond * time.Duration(c.Params.EpochMillis))
					go CliMsgMnt(c,serverSendMessage)
					go Recv_Data_Converge(s,c)
				}
				c:=s.Clis[conn]
				ack:=NewAck(conn,-1)
				//fmt.Println("Serv send ack:"+ack.String())
				serverSendMessage(c, ack)
				s.mutex.Unlock()
			} else{
				s.mutex.Lock()
				c, ok := s.Clis[msg.ConnID]
				s.mutex.Unlock()
				if !ok {
					fmt.Printf("client with connID %d is not exist.", msg.ConnID)
					continue
				}
				c.RecvWin<-msg
			}
		}
	}
}

func serverSendMessage(c *client, msg *Message) error {
	//fmt.Println("Server Sent Msg:"+msg.String())
	writeBytes, err := json.Marshal(msg)
	if err != nil {
		//fmt.Println("Error server json Marshal.", err.Error())
		return err
	}
	_, err = c.Conn.WriteToUDP(writeBytes, c.UDPADDr)
	if err != nil {
		//fmt.Println("Error server WriteToUDP.", err.Error())
		return err
	}
	//fmt.Println("Server send: " + msg.String(), time.Now())
	return nil
}

func (s *server) serverRecvMessage(readBytes []byte) (*Message, *lspnet.UDPAddr, error) {
	s.Conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(s.Params.EpochMillis)))
	readSize, rAddr, err := s.Conn.ReadFromUDP(readBytes)
	if err != nil {
		//fmt.Println("Error server ReadFromUDP.", err.Error())
		return nil, nil, err
	}
	var msg Message
	err = json.Unmarshal(readBytes[:readSize], &msg)
	if err != nil {
		//fmt.Println("Error json.Unmarshal.", err.Error())
		return nil, nil, err
	}
	//fmt.Println("Server recv:" + msg.String(), time.Now())
	return &msg, rAddr, nil
}