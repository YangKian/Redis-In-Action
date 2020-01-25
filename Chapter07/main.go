package main

import (
	"fmt"
	"reflect"
)

func copyInsert(slice interface{}, pos int, value interface{}) interface{} {
	v := reflect.ValueOf(slice)
	v = reflect.Append(v, reflect.ValueOf(value))
	reflect.Copy(v.Slice(pos+1, v.Len()), v.Slice(pos, v.Len()))
	v.Index(pos).Set(reflect.ValueOf(value))
	return v.Interface()
}

func Insert(slice interface{}, pos int, value interface{}) interface{} {

	v := reflect.ValueOf(slice)

	ne := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(value)), 1, 1)

	ne.Index(0).Set(reflect.ValueOf(value))
	v = reflect.AppendSlice(v.Slice(0, pos), reflect.AppendSlice(ne, v.Slice(pos, v.Len())))

	return v.Interface()
}
func main() {
	slice := []int{1, 2, 3, 4, 5}

	slice = append(slice[:2], slice[1:]...)
	slice[1] = 9
	fmt.Println(slice)
}
