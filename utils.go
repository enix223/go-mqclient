package mqclient

// Fn call back function
type Fn func() bool

// WaitUntil wait until for fn returning true or done
// is close or message arrived from done channel
func WaitUntil(done <-chan struct{}, fn Fn) {
	for {
		select {
		case <-done:
			return
		default:
			if fn() {
				return
			}
		}
	}
}
