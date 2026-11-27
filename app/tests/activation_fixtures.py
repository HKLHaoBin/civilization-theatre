"""Actual Host terminal fixture; scripted request seam, no SDK claim."""
from app.coordinator_scheduler import ActivationRef
from app.tests import test_actor_execution as fixtures

def close_activations(world,host,owner,window):
    host._choose=lambda view,seen:({'action':'wait'},seen)
    host._request=fixtures.ActorExecutionTests.result
    results=[]
    while True:
        row=world.next_activation(window['window_id'],coordinator=owner)
        if row is None:break
        ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
        results.append(host.act(ref.person_id,coordinator=owner,activation=ref))
    return results
