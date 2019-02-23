package bamboo

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"sync"
)

// Series for handling column level data of the data frame
type Series struct {
	data  []interface{}
	stype string // Expected type of data in this series
}

// Lambda function caller which will concurrently iterate over the data and execute a function literal
// Ensure that all methods / data manipulation used in the function literal are thread safe
func (this *Series) Lambda(ctx context.Context, lambda func(ctx context.Context, column interface{}) (columnOut interface{}, override bool)) (dataOut []interface{}, err error) {
	var wg = sync.WaitGroup{}

	if this.data != nil {
		// Ensure the lambda function is not nil
		if lambda != nil {

			dataOut = make([]interface{}, len(this.data))

			// Iterate over each row in the series
			for index := range this.data {
				select {
				case <-ctx.Done():
					// Break out of the loop because the context has been cancelled or timed out
					err = errors.Errorf("processing of data for lambda stopped prematurely due to closed context")
					break
				default:
					wg.Add(1)
					go func(index int) {
						// TODO: Add handler here for panics
						defer wg.Done()

						// Execute the lambda function
						var newValue interface{}
						var override bool

						if newValue, override = lambda(ctx, this.data[index]); override {
							this.data[index] = newValue
						}

						dataOut[index] = newValue
					}(index)
				}
			}

			// Wait for processing to finish
			wg.Wait()
		} else {
			err = errors.Errorf("nil lambda function passed to series")
		}
	} else {
		err = errors.Errorf("the data is nil in the series")
	}

	return dataOut, err
}

// Ensure the data is a slice of data
func (this *Series) SetData(data interface{}) (err error) {
	// TODO: Determine how to handle nil data here

	switch reflect.TypeOf(data).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(data)

		this.data = make([]interface{}, s.Len())

		for i := 0; i < s.Len(); i++ {
			// TODO: Attempt to determine the type of the data being input here
			//switch reflect.TypeOf(data).Kind() {
			//case reflect.Slice:
			//
			//}
			this.data[i] = s.Index(i).Interface()
		}
	default:
		err = errors.New("series data must be set using a slice")
	}

	return err
}

func (this *Series) GetData() (data []interface{}) {
	return this.data
}

func (this *Series) Get(index int) (value interface{}) {
	return this.data[index]
}

func (this *Series) Min() (err error) {
	return err
}

func (this *Series) Filter() (err error) {
	return err
}

// Validate the series struct
func (this *Series) Validate() (valid bool) {

	// Valid only if the data is not nil
	if this.data != nil {
		valid = true
	}

	return valid
}

func (this *Series) Print() {
	for _, value := range this.data {
		fmt.Println(value)
	}
}
