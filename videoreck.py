import cv2
import streamlit as st
import numpy as np
import tempfile
import os

# Simulated violence timestamps
def simulate_violence_detection():
    return [2.5, 5.0, 7.5]  # Violent moments in seconds

# Process the video and mark violence timestamps
def mark_violence_on_video(video_path, violence_timestamps):
    cap = cv2.VideoCapture(video_path)
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    # Check if the video opened successfully
    if not cap.isOpened():
        st.error("Error: Could not open the video.")
        return None

    # Create a temporary file with the correct extension
    temp_video_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
    temp_video_path = temp_video_file.name  # Get the file path
    temp_video_file.close()  # Close so OpenCV can access it

    # Define the codec and create a VideoWriter object
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')  # 'mp4v' is for .mp4 files
    out = cv2.VideoWriter(temp_video_path, fourcc, fps, (frame_width, frame_height))

    frame_count = 0
    success, frame = cap.read()

    # Loop through each frame and process it
    while success:
        # Calculate current time in seconds
        current_time = frame_count / fps

        # Check if current time is close to a violence timestamp
        if any(abs(current_time - t) < 1.0 for t in violence_timestamps):
            # Overlay a red rectangle on the top of the frame
            cv2.rectangle(frame, (0, 0), (frame_width, 50), (0, 0, 255), -1)  # Red marker

        # Write the processed frame into the output video
        out.write(frame)

        # Debug: Display the frame being processed
        st.image(frame, caption=f"Processing frame {frame_count}", channels="BGR")

        # Read the next frame
        success, frame = cap.read()
        frame_count += 1

    # Release the video objects
    cap.release()
    out.release()

    return temp_video_path

# Example usage
video_path = 'sample_video.mp4'  # Replace with your video path
violence_timestamps = simulate_violence_detection()  # Simulated timestamps
processed_video_path = mark_violence_on_video(video_path, violence_timestamps)

# Check if the processed video was created successfully
if processed_video_path:
    # Display the processed video in Streamlit
    st.video(processed_video_path)

    # Clean up the temp file after displaying the video
    if os.path.exists(processed_video_path):
        os.remove(processed_video_path)
