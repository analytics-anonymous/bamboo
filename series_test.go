package bamboo

import (
	"context"
	"testing"
)

func TestSeries_Lambda(t *testing.T) {
	tables := []struct{
		data_in []int
		data_out []int
		lambda func(ctx context.Context, column interface{})
	} {
		{
			[]int{1,2,3},
			[]int{2,4,6},
			func(ctx context.Context, column interface{}) {
				if val, ok := column.(*int); ok {
					var new = *val * 2
					column = &new
				}
			},
		},

	}

	for _, table := range tables {
		var series = Series{}

		series.SetData(table.data_in)

		series.Lambda(context.Background(), table.lambda)

		for index := range table.data_out {
			if table.data_in[index] != table.data_out[index] {
				t.Errorf("Got [%v], Expected [%v]\n", table.data_in[index], table.data_out[index])
			}
		}
	}
}

func BenchmarkSeries_Lambda(b *testing.B) {

	var data_in = []int{1,2,3}
	var lambda = func(ctx context.Context, column interface{}) {
		if val, ok := column.(*int); ok {
			var new = *val * 2
			column = &new
		}
	}

	var series = Series{}
	series.SetData(data_in)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		series.Lambda(context.Background(), lambda)
	}
}