package bamboo

import "github.com/pkg/errors"

// Series for handling column level data of the data frame
type Series struct {
	data []interface{}
}

// Lambda function caller which will iterate over the data and execute a function literal
func (this Series) Lambda(lambda func(column interface{}))(err error) {

	if lambda != nil {
		for index := range this.data {
			lambda(this.data[index])
		}
	} else {
		err = errors.Errorf("nil lambda function passed to series")
	}

	return err
}

func (this Series) Min() {

}

func (this Series) Filter() {

}