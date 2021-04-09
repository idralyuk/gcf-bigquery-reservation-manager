"""Google Cloud Function to manage BigQuery Reservations."""

# MIT License
#
# Copyright (c) 2021 Igor Dralyuk
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import functions_framework
import random
import string

from datetime import datetime
from flask import jsonify, make_response, Request
from google.api_core.retry import Retry
from google.api_core.exceptions import GoogleAPICallError, FailedPrecondition
from google.cloud.bigquery_reservation_v1 import Assignment, CapacityCommitment, Reservation, CreateAssignmentRequest, CreateCapacityCommitmentRequest, CreateReservationRequest, ListAssignmentsRequest, ListCapacityCommitmentsRequest, ListReservationsRequest, DeleteAssignmentRequest, DeleteCapacityCommitmentRequest, DeleteReservationRequest, ReservationServiceClient

__version__ = '0.1.0'
__author__ = 'Igor Dralyuk'
__license__ = 'MIT'

@functions_framework.errorhandler(Exception)
def handle_exception(e):
    if hasattr(e, 'message'):
        return make_response(jsonify({'message': e.message, 'severity': 'error'}, ), 500)
    else:
        return make_response(jsonify({'message': str(e), 'severity': 'error'}, ), 500)

def main_http(request: Request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """

    data = request.get_json(force=True, silent=True)
    
    if data is None:
        print('Error: json request is required')
        return make_response(jsonify({'message': 'json request is required', 'severity': 'error'}), 400)

    if 'project_id' not in data:
        print('Error: project_id is required in the json request')
        return make_response(jsonify({'message': 'project_id is required in the json request', 'severity': 'error'}), 400)
    else:
        project_id = data['project_id']
    
    location = data.get('location', 'EU')
    slots = data.get('slots', 100)
    
    client = ReservationServiceClient()

    if 'operation' not in data:
        return make_response(jsonify({'message': "Available operations are 'report', 'cleanup' and 'purchase'", 'severity': 'info'}), 200)
    elif 'report' == data['operation']:
        return report_http(request, client, project_id, location)
    elif 'cleanup' == data['operation']:
        return cleanup_http(request, client, project_id, location)
    elif 'purchase' == data['operation']:
        return purchase_http(request, client, project_id, location, int(slots))
    else:
        return make_response(jsonify({'message': f"Unsupported operation '{data['operation']}'", 'severity': 'info'}), 400)        

def report_http(request: Request, client: ReservationServiceClient, project_id: str, location: str):
    log = report(client, project_id, location)
    print(f"Successfully ran report in {location} for project {project_id}")
    return make_response(
        jsonify({
            'message': f"Successfully ran report in {location} for project {project_id}",
            'log': log,
            'severity': 'info'
        }),
        200)        

def cleanup_http(request: Request, client: ReservationServiceClient, project_id: str, location: str):
    log = cleanup(client, project_id, location)
    print(f"Successfully ran cleanup in project {project_id} located in {location}")
    return make_response(
        jsonify({
            'message': f"Successfully ran cleanup in project {project_id} located in {location}",
            'log': log,
            'severity': 'info'
        }),
        200)

def purchase_http(request: Request, client: ReservationServiceClient, project_id: str, location: str, slots: int):    
    commitment = purchase_commitment(client, project_id, location, slots)
    print(f"Successfully purchased commitment for {slots} slots in project {project_id} located in {location}")

    try:            
        ra = create_reservation_and_assignment(client, project_id, location, slots)
        
        return make_response(
            jsonify(
                {'message': f"Successfully purchased commitment for {slots} slots in project {project_id} located in {location}", 'commitment': commitment.name, 'reservation': ra['reservation'].name, 'assignment': ra['assignment'].name, 'severity': 'info'}),
                200)
    except Exception as e:
        print(f"Error while creating reservation and assignment, rolling back commitment creation")
        delete_commitment(client, commitment.name)
        raise e

def get_random_string(length: int = 8):
    random_str = ''.join(random.choice(string.ascii_lowercase) for i in range(length))
    return random_str

def get_reservation_name():
    name = datetime.today().strftime('reservation-%Y%m%d-%H%M%S%M') + '-' + get_random_string()
    return name

def report(client: ReservationServiceClient, project_id: str, location: str):
    log = []

    for reservation in get_reservations(client, project_id, location):
        log.append(f"Reservation {reservation.name}")
        for assignment in get_assignments(client, reservation.name):
                log.append(f"Assignment {assignment.name}")
    
    for commitment in get_commitments(client, project_id, location):
            log.append(f"Commitment {commitment.name}")
    
    return log

def cleanup(client: ReservationServiceClient, project_id: str, location: str):
    log = []
    
    for reservation in get_reservations(client, project_id, location):
        for assignment in get_assignments(client, reservation.name):
            log.append(delete_assignment(client, assignment.name))
        log.append(delete_reservation(client, reservation.name))
    
    for commitment in get_commitments(client, project_id, location):
        log.append(delete_commitment(client, commitment.name))
    
    return log

def get_commitments(client: ReservationServiceClient, project_id: str, location: str):
    req = ListCapacityCommitmentsRequest(parent=client.common_location_path(project_id, location))
    commitments = []
    
    for commitment in client.list_capacity_commitments(request=req):
        commitments.append(commitment)
    
    return commitments

def get_reservations(client: ReservationServiceClient, project_id: str, location: str):
    req = ListReservationsRequest(parent=client.common_location_path(project_id, location))
    reservations = []
    
    for reservation in client.list_reservations(request=req):
        reservations.append(reservation)
    
    return reservations

def get_assignments(client: ReservationServiceClient, parent: str):
    req = ListAssignmentsRequest(parent=parent)
    assignments = []
    
    for assignment in client.list_assignments(request=req):
        assignments.append(assignment)
    
    return assignments

def delete_assignment(client: ReservationServiceClient, assignment_name: str):
    print(f"Deleting assignment {assignment_name}")
    
    req = DeleteAssignmentRequest(name=assignment_name)
    client.delete_assignment(request=req)

    return f"Deleted assignment {assignment_name}"

def delete_reservation(client: ReservationServiceClient, reservation_name: str):
    print(f"Deleting reservation {reservation_name}")
    
    req = DeleteReservationRequest(name=reservation_name)
    client.delete_reservation(request=req)
    
    return f"Deleted reservation {reservation_name}"

def delete_commitment(client: ReservationServiceClient, commitment_name: str):
    print(f"Deleting commitment {commitment_name}")
    
    req = DeleteCapacityCommitmentRequest(name=commitment_name)
    client.delete_capacity_commitment(request=req, retry=Retry(deadline=90, predicate=Exception, maximum=2))
    
    return f"Deleted commitment {commitment_name}"

def purchase_commitment(client: ReservationServiceClient, project_id: str, location: str = 'EU', slots: int = 100, plan='FLEX'):
    print(f"Purchasing commitment for {slots} {plan} slots in project {project_id} in {location}")
    capacity_commitment = CapacityCommitment(plan=plan, slot_count=slots)
    
    req = CreateCapacityCommitmentRequest(
        parent=client.common_location_path(project_id, location),
        capacity_commitment=capacity_commitment,
    )
    commitment = client.create_capacity_commitment(request=req)
    return commitment

def create_reservation_and_assignment(client: ReservationServiceClient, project_id: str, location: str = 'EU', slots=100, reservation_name: str = get_reservation_name()):
    print(f"Creating reservation {reservation_name} for {slots} slots in project {project_id} in {location}")
    reservation_config = Reservation(slot_capacity=slots, ignore_idle_slots=False)

    req = CreateReservationRequest(
        parent=client.common_location_path(project_id, location),
        reservation_id=reservation_name,
        reservation=reservation_config,
    )
    reservation = client.create_reservation(request=req)

    try:
        assignment = create_assignment(client, project_id, reservation.name)
        return { 'reservation': reservation, 'assignment': assignment }
    except Exception as e:
        print(f"ERROR: {e.message}")
        print("Error while creating assignment, rolling back reservation creation")
        delete_reservation(client, reservation.name)
        raise

def create_assignment(client: ReservationServiceClient, project_id: str, reservation_name: str):
    print(f"Creating assignment for {reservation_name} in project {project_id}")

    assignment_config = Assignment(job_type='QUERY', assignee='projects/{}'.format(project_id))
    req = CreateAssignmentRequest(
        parent=reservation_name,
        assignment=assignment_config,
    )
    assignment = client.create_assignment(request=req)

    return assignment    