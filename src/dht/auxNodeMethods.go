/* ---- auxiliary methods ----*/

package chord

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"
)

// method Ping() ping the given address
func (o *Node) Ping(addr string) bool {
	return Ping(addr)
}

// method PutValue() puts a Value into the map
func (o *Node) PutValue(kv KVPair, success *bool) error {
	o.Data.lock.Lock()
	o.Data.Map[kv.Key] = kv.Value
	o.Data.lock.Unlock()
	*success = true
	return nil
}

// method GetValue() returns Value of a Key
func (o *Node) GetValue(key string, value *string) error {
	o.Data.lock.Lock()
	str, ok := o.Data.Map[key]
	o.Data.lock.Unlock()
	*value = str
	if ok == false {
		return errors.New("Get not found ")
	}
	return nil
}

// method DeleteValue() deletes a Value
func (o *Node) DeleteValue(key string, success *bool) error {
	o.Data.lock.Lock()
	_, ok := o.Data.Map[key]
	if ok == true {
		delete(o.Data.Map, key)
		*success = true
	} else {
		*success = false
	}
	o.Data.lock.Unlock()
	return nil
}

// method PutValueSuccessor() put value to the successor's DataPre
func (o *Node) PutValueSuccessor(kv KVPair, success *bool) error {
	err := o.FixSuccessors()
	if err != nil {
		return err
	}
	if Ping(o.Successor[1].Addr) == false {
		return errors.New("Error: Not connected[6] ")
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		return err
	}
	err = client.Call("RPCNode.PutValueDataPre", kv, success)
	if err != nil {
		_ = client.Close()
		return err
	}
	err = client.Close()
	if err != nil {
		return err
	}
	*success = true
	return nil
}

func (o *Node) DeleteValueSuccessor(key string, success *bool) error {
	err := o.FixSuccessors()
	if err != nil {
		return err
	}
	if Ping(o.Successor[1].Addr) == false {
		return errors.New("Error: Not connected[7] ")
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		return err
	}
	err = client.Call("RPCNode.DeleteValueDataPre", key, success)
	if err != nil {
		_ = client.Close()
		return err
	}
	err = client.Close()
	if err != nil {
		return err
	}
	return nil
}

func (o *Node) PutValueDataPre(kv KVPair, success *bool) error {
	o.DataPre.lock.Lock()
	o.DataPre.Map[kv.Key] = kv.Value
	o.DataPre.lock.Unlock()
	*success = true
	return nil
}

func (o *Node) DeleteValueDataPre(key string, success *bool) error {
	o.DataPre.lock.Lock()
	delete(o.DataPre.Map, key)
	o.DataPre.lock.Unlock()
	*success = true
	return nil
}

// method GetPredecessor() returns an Edge pointing to the predecessor of the current node
func (o *Node) GetPredecessor(args int, res *Edge) error {
	if o.Predecessor == nil {
		return errors.New("GetPredecessor: predecessor not found ")
	}
	*res = Edge{o.Predecessor.Addr, new(big.Int).Set(o.Predecessor.ID)}
	return nil
}

// method GetSuccessorList() returns a list of successors of a node
func (o *Node) GetSuccessorList(args int, res *[successorListLen + 1]Edge) error {
	o.sLock.Lock()
	for i := 1; i <= successorListLen; i++ {
		(*res)[i] = Edge{o.Successor[i].Addr, new(big.Int).Set(o.Successor[i].ID)}
	}
	o.sLock.Unlock()
	return nil
}

// method MoveAllDataToSuccessor(successor) moves the data of the current node to its successor
func (o *Node) MoveAllDataToSuccessor() {
	if Ping(o.Successor[1].Addr) == false {
		fmt.Println("Error: Not connected[1]")
		return
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error[1]: ", err)
		return
	}

	err = client.Call("RPCNode.QuitMoveData", o.Data, new(int))
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Calling Node.QuitMoveData: ", err)
		return
	}
	err = client.Call("RPCNode.QuitMoveDataPre", o.DataPre, new(int))
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Calling Node.QuitMoveDataPre: ", err)
		return
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return
	}
}

// method MoveKVPairs() called when Join(), move successor's data to my data
func (o *Node) MoveKVPairs(newNode *big.Int, res *map[string]string) error {
	cnt := 0
	for o.Predecessor == nil && cnt < FailTimes {
		time.Sleep(Second)
		cnt++
	}
	if cnt == FailTimes {
		return errors.New("Predecessor not found when Join ")
	}
	o.DataPre.lock.Lock()
	o.Data.lock.Lock()
	o.DataPre.Map = make(map[string]string)
	for k, v := range o.Data.Map {
		KID := hashString(k)
		if between(o.Predecessor.ID, KID, newNode, true) {
			(*res)[k] = v
			o.DataPre.Map[k] = v
		}
	}
	for k := range *res {
		delete(o.Data.Map, k)
	}
	o.Data.lock.Unlock()
	o.DataPre.lock.Unlock()
	return nil
}

// method MoveDataPre() called when Join(), move successor's DataPre to my Data
func (o *Node) MoveDataPre(args int, res *map[string]string) error {
	if args == 0 {
		o.DataPre.lock.Lock()
		for k, v := range o.DataPre.Map {
			(*res)[k] = v
		}
		o.DataPre.lock.Unlock()
	} else {
		o.Data.lock.Lock()
		for k, v := range o.Data.Map {
			(*res)[k] = v
		}
		o.Data.lock.Unlock()
	}
	return nil
}

// method QuitMoveData()
func (o *Node) QuitMoveData(Data KVMap, res *int) error {
	err := o.FixSuccessors()
	if err != nil {
		return err
	}
	if !Ping(o.Successor[1].Addr) {
		return errors.New("Error: Not connected[8] ")
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		return err
	}
	Data.lock.Lock()
	o.Data.lock.Lock()
	for k, v := range Data.Map {
		o.Data.Map[k] = v
		err = client.Call("RPCNode.PutValueDataPre", KVPair{k, v}, new(bool))
		if err != nil {
			o.Data.lock.Unlock()
			Data.lock.Unlock()
			_ = client.Close()
			return err
		}
	}
	o.Data.lock.Unlock()
	Data.lock.Unlock()

	err = client.Close()
	if err != nil {
		return err
	}
	return nil
}

// method QuitMoveDataPre()
func (o *Node) QuitMoveDataPre(DataPre KVMap, res *int) error {
	o.DataPre.lock.Lock()
	//o.DataPre.Map = make(map[string]string)
	o.DataPre.Map = DataPre.Map
	o.DataPre.lock.Unlock()
	return nil
}

// method SetSuccessor()
func (o *Node) SetSuccessor(edge Edge, res *int) error {
	o.Successor[1] = edge
	var list [successorListLen + 1]Edge

	if Ping(o.Successor[1].Addr) == false {
		fmt.Println("Error: Not connected[1]")
		return errors.New("Not connected[1] ")
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error[4]: ", err)
		return err
	}
	err = client.Call("RPCNode.GetSuccessorList", 0, &list)
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Call GetSuccessorList Error", err)
		return err
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return err
	}

	o.sLock.Lock()
	for i := 2; i <= successorListLen; i++ {
		o.Successor[i] = list[i-1]
	}
	o.sLock.Unlock()
	return nil
}

// method SetPredecessor()
func (o *Node) SetPredecessor(edge Edge, res *int) error {
	o.Predecessor = &edge
	//if Ping(o.Predecessor.Addr) == false {
	//	return errors.New("Error: Not connected[5] ")
	//}
	//client, err := Dial(o.Predecessor.Addr)
	//if err != nil {
	//	return err
	//}
	//o.DataPre.lock.Lock()
	//o.DataPre.Map = make(map[string]string)
	//err = client.Call("RPCNode.MoveDataPre", 0, &o.DataPre.Map)
	//o.DataPre.lock.Unlock()
	//err = client.Close()
	//if err != nil {
	//	return err
	//}
	return nil
}

// method simpleStabilize() stabilize once
func (o *Node) simpleStabilize() {
	err := o.FixSuccessors()
	if err != nil {
		return
	}
	oldSuccessor := o.Successor[1]

	if Ping(o.Successor[1].Addr) == false {
		//fmt.Println("Error: Not connected[2]")
		return
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		//fmt.Println("Error: Dialing error[2]: ", err)
		return
	}

	defer func() {
		err = client.Call("RPCNode.Notify", &Edge{o.Addr, new(big.Int).Set(o.ID)}, new(int))
		if err != nil {
			_ = client.Close()
			fmt.Println("Error: Node.Notify error: ", err)
			return
		}

		var list [successorListLen + 1]Edge
		err = client.Call("RPCNode.GetSuccessorList", 0, &list)
		if err != nil {
			_ = client.Close()
			fmt.Println("Error: Call GetSuccessorList Error", err)
			return
		}
		o.sLock.Lock()
		for i := 2; i <= successorListLen; i++ {
			o.Successor[i] = list[i-1]
		}
		o.sLock.Unlock()

		err = client.Close()
		if err != nil {
			fmt.Println("Error: Close client error: ", err)
			return
		}
	}()

	var successorPre Edge
	err = client.Call("RPCNode.GetPredecessor", 0, &successorPre)
	if err != nil {
		//fmt.Println("Error: Calling Node.GetPredecessor: ", err, o.Addr, "successor", o.Successor[1].Addr)
		return
	}
	if !Ping(successorPre.Addr) {
		return
	}

	if between(o.ID, successorPre.ID, o.Successor[1].ID, false) {
		o.sLock.Lock()
		o.Successor[1] = successorPre
		err = client.Close()
		if err != nil {
			o.sLock.Unlock()
			fmt.Println("Error: Close client error: ", err)
			return
		}

		if Ping(o.Successor[1].Addr) == false {
			fmt.Println("Error: Not connected[3]", oldSuccessor)
			return
		}
		client, err = Dial(o.Successor[1].Addr)
		o.sLock.Unlock()
		if err != nil {
			fmt.Println("Error: Dialing error[3]: ", err, o.Addr, "successorPre", successorPre.Addr)
			return
		}
	}
}

// method FixSuccessors fixes the successor list
func (o *Node) FixSuccessors() error {
	if o.Successor[1].Addr == o.Addr {
		return nil
	}
	o.sLock.Lock()

	var p int
	for p = 1; p <= successorListLen; p++ {
		if o.Ping(o.Successor[p].Addr) {
			break
		}
	}
	if p == successorListLen+1 {
		o.sLock.Unlock()
		return errors.New("Error: No valid successor!!!! ")
	}

	if p == 1 {
		o.sLock.Unlock()
		return nil
	}

	o.Successor[1] = o.Successor[p]
	o.sLock.Unlock()
	var list [successorListLen + 1]Edge
	if Ping(o.Successor[1].Addr) == false {
		fmt.Println("Error: Not connected[4]")
		return nil
	}
	client, err := Dial(o.Successor[1].Addr)
	if err != nil {
		fmt.Println("Error: Dialing error[4]: ", err)
		return nil
	}

	err = client.Call("RPCNode.GetSuccessorList", 0, &list)
	if err != nil {
		_ = client.Close()
		fmt.Println("Error: Call GetSuccessorList Error", err)
		return nil
	}
	err = client.Close()
	if err != nil {
		fmt.Println("Error: Close client error: ", err)
		return nil
	}

	o.sLock.Lock()
	for i := 2; i <= successorListLen; i++ {
		o.Successor[i] = list[i-1]
	}
	o.sLock.Unlock()
	return nil
}

func (o *Node) AgreeJoin(addr string, agree *bool) error {
	fmt.Println("A user in", addr, "wants to join the chat room. Do you agree?(y/n)")
	fmt.Println("If you don't response within 20 seconds, we assume that you enter \"n\"." +
		" (Due to some reasons, you need to enter your response TWICE. Sorry.)")
	o.PrintLock.Lock()
	ch := make(chan bool)
	stopCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopCh:
				return
			default:
				fmt.Printf(" >>> ")
				reader := bufio.NewReader(os.Stdin)
				text, err := reader.ReadString('\n')
				if err != nil {
					log.Fatal(err)
				}
				text = text[:len(text)-1]
				if text[0] == 'y' || text[0] == 'Y' {
					ch <- true
					return
				} else if text[0] == 'n' || text[0] == 'N' {
					ch <- false
					return
				} else {
					fmt.Println("Invalid response. Please enter \"y\" or \"n\".")
				}
			}
		}
	}()
	select {
	case ok := <-ch:
		*agree = ok
	case <-time.After(20 * time.Second):
		fmt.Println("We assume that you enter \"n\" because you didn't response within 20 seconds.")
		*agree = false
		stopCh <- struct{}{}
	}
	*agree = true
	o.PrintLock.Unlock()
	return nil
}

func (o *Node) PrintMessage(pair StrPair, res *int) error {
	if o.Successor[1].Addr != o.Addr {
		_ = o.FixSuccessors()
	}
	if o.Successor[1].Addr != pair.Addr {
		go func() {
			if Ping(o.Successor[1].Addr) == false {
				return
			}
			client, err := Dial(o.Successor[1].Addr)
			if err != nil {
				return
			}
			err = client.Call("RPCNode.PrintMessage", pair, new(int))
			_ = client.Close()
		}()
	}
	fmt.Println(pair.Str)
	return nil
}
