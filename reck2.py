import streamlit as st
import streamlit.components.v1 as components

# Example function to get violence timestamps
def get_violence_timestamps():
    return [2.5, 5.0, 7.5]  # Example timestamps in seconds

# HTML template for the video player
def video_player_html(video_url, timestamps):
    timestamp_marks = ",".join([f"{int(t*100)/100}" for t in timestamps])  # Format timestamps
    return f"""
    <video id="videoPlayer" width="640" height="360" controls>
      <source src="{video_url}" type="video/mp4">
      Your browser does not support the video tag.
    </video>
    <style>
        /* Style for the timestamp markers */
        .videoMarker {{
            position: absolute;
            background-color: red;
            width: 2px;
            height: 100%;
        }}
    </style>
    <script>
        const video = document.getElementById('videoPlayer');
        const timestamps = [{timestamp_marks}];

        // Function to add markers on the video timeline
        function addMarkers() {{
            const duration = video.duration;
            const progressBar = video.querySelector('input[type="range"]');
            timestamps.forEach(function(ts) {{
                const marker = document.createElement('div');
                marker.classList.add('videoMarker');
                marker.style.left = (ts / duration * 100) + '%';
                document.body.appendChild(marker);
            }});
        }}

        video.addEventListener('loadedmetadata', addMarkers);
    </script>
    """

# Path to the video file (replace with your own video)
video_url = "https://sample-videos.com/video123/mp4/720/big_buck_bunny_720p_1mb.mp4"  # Replace with actual URL or file path
timestamps = get_violence_timestamps()  # Fetch violence timestamps

# Display the custom video player with markers in Streamlit
html_code = video_player_html(video_url, timestamps)
components.html(html_code, height=400)
