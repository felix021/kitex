/*
 * Copyright 2024 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ttheaderstreaming

import "sync/atomic"

const (
	stateNew             = 0
	stateMetaReceived    = 1
	stateHeaderReceived  = 2
	stateDataReceived    = 3
	stateTrailerReceived = 4
)

type state struct {
	value uint32
}

func newState() *state {
	return &state{
		value: stateNew,
	}
}

func (s *state) get() uint32 {
	return atomic.LoadUint32(&s.value)
}

func (s *state) setMetaReceived() (old uint32, allow bool) {
	old = atomic.SwapUint32(&s.value, stateMetaReceived)
	return old, old == stateNew
}

func (s *state) setHeaderReceived() (old uint32, allow bool) {
	old = atomic.SwapUint32(&s.value, stateHeaderReceived)
	return old, old < stateHeaderReceived
}

func (s *state) setDataReceived() (old uint32, allow bool) {
	old = atomic.SwapUint32(&s.value, stateDataReceived)
	return old, old == stateHeaderReceived || old == stateDataReceived
}

func (s *state) setTrailerReceived() (old uint32, allow bool) {
	old = atomic.SwapUint32(&s.value, stateTrailerReceived)
	return old, old == stateHeaderReceived || old == stateDataReceived
}
