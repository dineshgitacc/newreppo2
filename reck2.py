import streamlit as st
import streamlit.components.v1 as components

# Function to get violence timestamps
def get_violence_timestamps():
    return [2.5, 5.0, 7.5]  # Example timestamps in seconds

# Example video file
video_file = "/path/to/your/video.mp4"  # Replace with your actual video path

# Use st.video() to serve the video
st.video(video_file)

# Generate timestamp markers with JS
def video_marker_js(timestamps):
    return f"""
    <script>
    const video = document.querySelector('video');
    const timestamps = [{','.join(map(str, timestamps))}];

    function addMarkers() {{
        timestamps.forEach(function(timestamp) {{
            const marker = document.createElement('div');
            marker.className = 'marker';
            marker.style.position = 'absolute';
            marker.style.left = (timestamp / video.duration * 100) + '%';
            marker.style.height = '5px';
            marker.style.width = '2px';
            marker.style.backgroundColor = 'red';
            marker.style.top = '0';
            document.querySelector('.stVideo').appendChild(marker);
        }});
    }}

    video.addEventListener('loadedmetadata', addMarkers);
    </script>
    """

# Example timestamps to mark on video
timestamps = get_violence_timestamps()

# Embed the JavaScript for video marker in Streamlit
components.html(video_marker_js(timestamps), height=0)
