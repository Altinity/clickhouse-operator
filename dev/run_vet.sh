#!/bin/bash

# The WithCancel, WithDeadline, and WithTimeout functions take a Context (the parent)
# and return a derived Context (the child) and a CancelFunc.
# Calling the CancelFunc cancels the child and its children, removes the parent's reference to the child,
# and stops any associated timers.
#
# Failing to call the CancelFunc leaks the child and its children until the parent is canceled or the timer fires.
# The go vet tool checks that CancelFuncs are used on all control-flow paths.
