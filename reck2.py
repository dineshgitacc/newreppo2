import cv2
import numpy as np
import streamlit as st
import tempfile
import os

# Function to create a simple video
def create_test_video():
    # Create a blank frame (black)
    frame_width, frame_height = 640, 480
    fps = 10  # Frames per second

    # Create a temporary file for the video
    temp_video_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
    temp_video_path = temp_video_file.name
    temp_video_file.close()

    # Define codec and create VideoWriter object
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # Codec for .mp4
    out = cv2.VideoWriter(temp_video_path, fourcc, fps, (frame_width, frame_height))

    for i in range(50):  # Create a video with 50 frames
        frame = np.zeros((frame_height, frame_width, 3), dtype=np.uint8)  # Black frame

        # Add a moving rectangle
        cv2.rectangle(frame, (i*10, 100), (i*10+50, 150), (0, 255, 0), -1)  # Green rectangle
        cv2.putText(frame, f"Frame {i+1}", (50, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)

        out.write(frame)  # Write the frame to the video

    out.release()
    return temp_video_path

# Test video creation
test_video_path = create_test_video()

# Display the test video in Streamlit
if test_video_path and os.path.exists(test_video_path):
    st.video(test_video_path)
    os.remove(test_video_path)  # Clean up
else:
    st.error("Error: Test video could not be created.")
