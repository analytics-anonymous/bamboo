package bamboo

import (
	"context"
	"github.com/pkg/errors"
	"sync"
)

// Series for handling column level data of the data frame
type Series struct {
	data []interface{}
}

// Lambda function caller which will concurrently iterate over the data and execute a function literal
// Ensure that all methods / data manipulation used in the function literal are thread safe
func (this Series) Lambda(ctx context.Context, lambda func(ctx context.Context, column interface{}))(err error) {
	var wg = sync.WaitGroup{}

	// Ensure the lambda function is not nil
	if lambda != nil {

		// Iterate over each row in the series
		for index := range this.data {
			wg.Add(1)

			select {
			case <- ctx.Done():
				// Break out of the loop because the context has been cancelled or timed out
				break
			default:
				go func() {
					// TODO: Add handler here for panics
					defer wg.Done()

					// Execute the lambda function
					lambda(ctx, this.data[index])
				}()
			}
		}

		// Wait for processing to finish
		wg.Wait()
	} else {
		err = errors.Errorf("nil lambda function passed to series")
	}

	return err
}

func (this Series) Min() {

}

func (this Series) Filter() {

}