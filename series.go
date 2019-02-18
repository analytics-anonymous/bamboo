package bamboo

import (
	"context"
	"github.com/pkg/errors"
	"sync"
)

// Series for handling column level data of the data frame
type Series struct {
	data []interface{}
	stype string // Expected type of data in this series
}

// Lambda function caller which will concurrently iterate over the data and execute a function literal
// Ensure that all methods / data manipulation used in the function literal are thread safe
func (this Series) Lambda(ctx context.Context, lambda func(ctx context.Context, column interface{}))(err error) {
	var wg = sync.WaitGroup{}

	if this.data != nil {
		// Ensure the lambda function is not nil
		if lambda != nil {

			// Iterate over each row in the series
			for _,value := range this.data {
				wg.Add(1)

				select {
				case <- ctx.Done():
					// Break out of the loop because the context has been cancelled or timed out
					break
				default:
					go func(value interface{}) {
						// TODO: Add handler here for panics
						defer wg.Done()

						// Execute the lambda function
						lambda(ctx, value)
					}(value)
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

	return err
}

func (this Series) SetData(data []interface{}) {
	this.data = data
}

func (this Series) GetData() (data []interface{}) {
	return this.data
}

func (this Series) Min() (err error) {
	return err
}

func (this Series) Filter() (err error) {
	return err
}

// Validate the series struct
func (this Series) Validate() (valid bool) {

	// Valid only if the data is not nil
	if this.data != nil {
		valid = true
	}

	return valid
}