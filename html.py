[4:57 PM, 9/9/2024] Tanush: <!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="styles.css">
  <title>Video with Markings</title>
</head>
<body>
  <div class="video-container">
    <video id="video" width="600" controls>
      <source src="your-video.mp4" type="video/mp4">
      Your browser does not support the video tag.
    </video>
    <div id="custom-timeline" class="custom-timeline">
      <div class="marker" data-time="5"></div>
      <div class="marker" data-time="10"></div>
      <div class="marker" data-time="20"></div>
    </div>
  </div>

  <script src="script.js"></script>
</body>
</html>
[4:57 PM, 9/9/2024] Tanush: .video-container {
  position: relative;
  width: 600px;
}

.custom-timeline {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  height: 5px;
  background-color: #ccc;
  cursor: pointer;
}

.marker {
  position: absolute;
  width: 2px;
  height: 100%;
  background-color: red;
}




document.addEventListener('DOMContentLoaded', function () {
  const video = document.getElementById('video');
  const timeline = document.getElementById('custom-timeline');

  // Example array of timestamps in "MM:SS" format
  const timestamps = ["00:05", "00:10", "00:20", "01:15"]; // Modify this array with your desired timestamps

  // Function to convert "MM:SS" format to seconds
  function convertToSeconds(time) {
    const parts = time.split(':');
    const minutes = parseInt(parts[0], 10);
    const seconds = parseInt(parts[1], 10);
    return minutes * 60 + seconds;
  }

  // Function to create markers dynamically
  function createMarkers() {
    timestamps.forEach(time => {
      const seconds = convertToSeconds(time); // Convert "MM:SS" to seconds
      const marker = document.createElement('div');
      marker.className = 'marker';
      marker.dataset.time = seconds;
      timeline.appendChild(marker);
    });
  }

  // Adjust the markers based on the video duration
  video.addEventListener('loadedmetadata', function () {
    createMarkers(); // Create markers after metadata is loaded
    const markers = document.querySelectorAll('.marker');
    markers.forEach(marker => {
      const time = marker.getAttribute('data-time');
      const percentage = (time / video.duration) * 100;
      marker.style.left = ${percentage}%;
    });
  });

  // Seek video when clicking on the custom timeline
  timeline.addEventListener('click', function (event) {
    const rect = timeline.getBoundingClientRect();
    const clickPosition = event.clientX - rect.left;
    const percentage = (clickPosition / timeline.offsetWidth);
    video.currentTime = percentage * video.duration;
  });

  // Jump to marker time when a marker is clicked
  timeline.addEventListener('click', function (event) {
    if (event.target.classList.contains('marker')) {
      event.stopPropagation(); // Prevent the parent timeline click handler
      const time = event.target.getAttribute('data-time');
      video.currentTime = time;
    }
  });
});
