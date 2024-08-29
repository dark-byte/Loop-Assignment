from flask import Blueprint, request, jsonify, send_file
import uuid
import io
from threading import Thread
from app.models import connect_to_database
from app.utils import create_report  # Import the create_report function

api = Blueprint('api', __name__)

@api.route('/trigger_report', methods=['POST'])
def trigger_report():
    # Generate a unique report ID
    report_id = str(uuid.uuid4())

    # Connect to the database
    conn = connect_to_database()
    cursor = conn.cursor()

    # Insert a new report entry into the database with the status 'Running'
    cursor.execute("INSERT INTO reports (report_id, status) VALUES (%s, %s)", (report_id, 'Running'))
    conn.commit()

    cursor.close()
    conn.close()
    
    # Start report generation in a background thread
    thread = Thread(target=create_report, args=(report_id,))
    thread.start()

    # Return the report ID to the client instantly
    return jsonify({"report_id": report_id, "status": "Running"})

@api.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')

    # Connect to the database
    conn = connect_to_database()
    cursor = conn.cursor()

    # Fetch the status of the report
    cursor.execute("SELECT status, report_data FROM reports WHERE report_id = %s", (report_id,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if not result:
        return jsonify({"error": "Invalid report ID"}), 404

    status, report_data = result

    if status == 'Running':
        return jsonify({"status": "Running"})

    output = io.BytesIO(report_data.encode('utf-8'))
    output.seek(0)
    return send_file(output, download_name=f'report_{report_id}.csv', as_attachment=True)
