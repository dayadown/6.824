package mr
//切片实现一个队列，支持查找，从中间出队的操作(对于普通类型)
import "errors"

type QueueNorm struct{
	items []interface{}
}

//入队
func (q *QueueNorm) Add(item interface{}){
	q.items=append(q.items,item)
}

//出队
func (q *QueueNorm) Out() (interface{}, error) {  
	if len(q.items) == 0 {  
		return nil, errors.New("QueueNorm is empty")  
	}  
	item := q.items[0]  
	q.items = q.items[1:]  
	return item, nil  
}

//查找某个元素的下标(对于普通类型)
func (q *QueueNorm) FindIndexNormal(item interface{}) int {  
	for i, v := range q.items {  
		if v == item {  
			return i  
		}  
	}  
	return -1  
}

//删除某一下标的值
func (q *QueueNorm) Delete(index int) error {  
	if index < 0 || index >= len(q.items) {  
		return errors.New("index out of bounds")  
	}  
	q.items = append(q.items[:index], q.items[index+1:]...)  
	return nil  
}

func (q *QueueNorm) IsEmpty() bool {
	return len(q.items)==0
}

func (q *QueueNorm) Size() int {
	return len(q.items)
}



