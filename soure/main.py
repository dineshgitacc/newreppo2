from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)

# Enable CORS for all routes
CORS(app)

@app.route('/video-info', methods=['GET'])
def video_info():
    # Define the response containing video URL and timestamps
    response_data = {
        'video_url': 'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4',  # Video URL
        'timestamps': [
            {'start': '00:00:03', 'end': '00:00:09'},
            {'start': '00:00:10', 'end': '00:00:20'},  # Example timestamp 1
            {'start': '00:00:45', 'end': '00:00:55'},  # Example timestamp 2
            {'start': '00:01:10', 'end': '00:04:25'}   # Example timestamp 3
        ]
    }
    
    # Return the data as JSON
    return jsonify(response_data)

if __name__ == '__main__':
    app.run(debug=True)
