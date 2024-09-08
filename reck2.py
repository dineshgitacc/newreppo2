import streamlit as st
import streamlit.components.v1 as components

# Example video file
video_url = "https://www.w3schools.com/html/mov_bbb.mp4"  # Replace with your own video URL or local file path

# Example violence timestamps in seconds
timestamps = [2.5, 5.0, 7.5]  # Replace with your own timestamp values

# HTML and JavaScript to display video and mark timestamps
video_html = f"""
    <video id="videoPlayer" width="640" height="360" controls>
      <source src="{video_url}" type="video/mp4">
      Your browser does not support the video tag.
    </video>
    <style>
      /* Style for the red line markers */
      .video-marker {{
        position: absolute;
        background-color: red;
        width: 2px;
        height: 100%;
        z-index: 10;
      }}
      #video-container {{
        position: relative;
        width: 640px;
        height: 360px;
      }}
    </style>
    <div id="video-container"></div>
    
    <script>
      const video = document.getElementById('videoPlayer');
      const container = document.getElementById('video-container');
      const timestamps = {timestamps};

      video.addEventListener('loadedmetadata', function() {{
        const videoDuration = video.duration;

        timestamps.forEach(function(timestamp) {{
          const marker = document.createElement('div');
          marker.classList.add('video-marker');
          marker.style.left = (timestamp / videoDuration * 100) + '%';
          container.appendChild(marker);
        }});
      }});
    </script>
"""

# Embed the video player with markers in Streamlit
components.html(video_html, height=400)
