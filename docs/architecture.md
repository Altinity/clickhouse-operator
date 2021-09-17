# Architecture


Like a Deployment , a StatefulSet manages Pods that are based on an **identical** container spec. 
Unlike a Deployment, a StatefulSet maintains a sticky identity for each of their Pods. 
These pods are created from the same spec, but are not interchangeable: each has a persistent identifier that it maintains across any rescheduling.
