package mr
//切片实现一个队列，支持查找，从中间出队的操作(对于自定义类型)
import "errors"

//限制队列中的元素类型,需实现equals方法
type Comparable interface {  
	Equals(other Comparable) bool  
}

type Queue struct{
	items []Comparable
}

//入队
func (q *Queue) Add(item Comparable){
	q.items=append(q.items,item)
}

//出队
func (q *Queue) Out() (Comparable, error) {  
	if len(q.items) == 0 {  
		return nil, errors.New("queue is empty")  
	}  
	item := q.items[0]  
	q.items = q.items[1:]  
	return item, nil  
}

//查找某个元素的下标(对于自定义类型)
func (q *Queue) FindIndex(item Comparable) int {
	for i,v:=range q.items {
		if v.Equals(item){
			return i
		}
	}
	return -1
}

//删除某一下标的值
func (q *Queue) Delete(index int) error {  
	if index < 0 || index >= len(q.items) {  
		return errors.New("index out of bounds")  
	}  
	q.items = append(q.items[:index], q.items[index+1:]...)  
	return nil  
}

func (q *Queue) IsEmpty() bool {
	return len(q.items)==0
}

func (q *Queue) Size() int {
	return len(q.items)
}



