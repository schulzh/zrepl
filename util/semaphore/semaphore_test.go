package semaphore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSemaphore(t *testing.T) {
	const numGoroutines = 10
	const concurrentSemaphore = 5
	const sleepTime = 1 * time.Second

	begin := time.Now()

	sem := New(concurrentSemaphore)

	var aquisitions struct {
		beforeT, afterT uint32
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			res, err := sem.Acquire(context.Background())
			require.NoError(t, err)
			defer res.Release()
			if time.Since(begin) > sleepTime {
				atomic.AddUint32(&aquisitions.beforeT, 1)
			} else {
				atomic.AddUint32(&aquisitions.afterT, 1)
			}
			time.Sleep(sleepTime)
		}()
	}

	wg.Wait()

	assert.True(t, aquisitions.beforeT == concurrentSemaphore)
	assert.True(t, aquisitions.afterT == numGoroutines-concurrentSemaphore)

}
