
import streamlit as st
from streamlit_drawable_canvas import st_canvas
from streamlit.components.v1 import html
import streamlit.components.v1 as components

import pandas as pd
import numpy as np
import boto3
from PIL import Image,ImageDraw,ImageFont
import json
import io

rekognition_client=boto3.client("rekognition",
                                aws_access_key_id="AKIA6ODUZ47FCEKZB55A",
                                aws_secret_access_key="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",
                                region_name="ap-south-1",verify=False)

test = []
# print(rekognition_client)  

# textract_client   =  boto3.client("textract",
#                                 aws_access_key_id="AKIA6ODUZ47FCEKZB55A",
#                                 aws_secret_access_key="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",
#                                 region_name="ap-south-1")                        

def detect_faces(image_bytes):
    # response = rekognition_client.detect_faces(Image={"Bytes": image_bytes}, Attributes=["ALL"])
    # with open('tmp_face1.json','w') as f:
    #         json.dump(response, f) 

    with open('tmp_face1.json') as f:
            response=json.load(f)  
    return response

# Function to draw bounding boxes and prepare face details
def draw_faces(image, face_details):
    draw = ImageDraw.Draw(image)
    face_coordinates = []

    for face in face_details:
        bounding_box = face["BoundingBox"]
        width, height = image.size
        left = bounding_box["Left"] * width
        top = bounding_box["Top"] * height
        right = left + (bounding_box["Width"] * width)
        bottom = top + (bounding_box["Height"] * height)

        st.write(bounding_box)
        st.write({"top":top,"right":right,"left":left,"bottom":bottom})
        print({"top":top,"right":right,"left":left,"bottom":bottom})
        test.append({"top":top,"right":right,"left":left,"bottom":bottom})
        # Draw the bounding box
        draw.rectangle([left, top, right, bottom], outline="red", width=4)

        # Collect face details
        details = []
        if face["Smile"]["Value"]:
            details.append(f"Smile: {face['Smile']['Confidence']:.2f}%")
        if face["Gender"]["Value"]:
            details.append(f"Gender: {face['Gender']['Value']}")
        if face["Eyeglasses"]["Value"]:
            details.append(f"Eyeglasses: {face['Eyeglasses']['Confidence']:.2f}%")
        if face["Sunglasses"]["Value"]:
            details.append(f"Sunglasses: {face['Sunglasses']['Confidence']:.2f}%")
        if face["Beard"]["Value"]:
            details.append(f"Beard: {face['Beard']['Confidence']:.2f}%")
        if face["Mustache"]["Value"]:
            details.append(f"Mustache: {face['Mustache']['Confidence']:.2f}%")
        if face["MouthOpen"]["Value"]:
            details.append(f"Mouth Open: {face['MouthOpen']['Confidence']:.2f}%")

        # Determine the dominant emotion
        dominant_emotion = max(face["Emotions"], key=lambda x: x["Confidence"])
        details.append(f"Emotion: {dominant_emotion['Type']} ({dominant_emotion['Confidence']:.2f}%)")
        details.append(f"Age: {face['AgeRange']['Low']} - {face['AgeRange']['High']}")

        # Store face coordinates and details
        face_coordinates.append({
            "left": left,
            "top": top,
            "right": right,
            "bottom": bottom,
            "details": "\n".join(details)
        })

    return image, face_coordinates

# Streamlit UI
st.title("Face Detection with AWS Rekognition")

# Upload image
upload_file = st.file_uploader("Upload an image", type=["jpg", "jpeg", "png"])
if upload_file is not None:
    image_bytes = upload_file.read()
    image = Image.open(upload_file)

    # Detect faces
    if st.button("Detect Faces"):
        response = detect_faces(image_bytes)
        face_details = response["FaceDetails"]

        # Draw faces and get face coordinates
        image_with_faces, face_coordinates = draw_faces(image.copy(), face_details)

        # Convert image to bytes for display
        buffered = io.BytesIO()
        image_with_faces.save(buffered, format="PNG")
        img_str = buffered.getvalue()

        # Display the image with bounding boxes
        st.image(image_with_faces, caption="Detected Faces", use_column_width=True)

        # st.image(image_with_faces, caption="Detected Faces", use_column_width=True)

        # Pass face coordinates to JavaScript
        # face_coordinates_json = json.dumps(face_coordinates)
        # st.markdown(f"""
        # <script>
        # const faceCoordinates = {face_coordinates_json};
        # const image = document.querySelector('img[alt="Detected Faces"]');

        # image.addEventListener('mousemove', function(event) {{
        #     const rect = image.getBoundingClientRect();
        #     const x = event.clientX - rect.left;
        #     const y = event.clientY - rect.top;

        #     let found = false;
        #     for (let i = 0; i < faceCoordinates.length; i++) {{
        #         const face = faceCoordinates[i];
        #         if (x >= face.left && x <= face.right && y >= face.top && y <= face.bottom) {{
        #             document.getElementById("desc").innerHTML = face.details;  // Display face details
        #             found = true;
        #             break;
        #         }}
        #     }}
        #     if (!found) {{
        #         document.getElementById("desc").innerHTML = "";  // Clear details if not hovering over a face
        #     }}
        # }});
        # </script>
        # """, unsafe_allow_html=True)

        # Create a description area for face details
        # st.markdown("<p id='desc' style='color: black;'></p>", unsafe_allow_html=True)

        # Pass face coordinates to JavaScript
        # st.markdown(f"""
        # <script>
        # const faceCoordinates = {json.dumps(face_coordinates)};
        # const image = document.querySelector('img[alt="Detected Faces"]');

        # image.addEventListener('click', function(event) {{
        #     const rect = image.getBoundingClientRect();
        #     const x = event.clientX - rect.left;
        #     const y = event.clientY - rect.top;

        #     for (let i = 0; i < faceCoordinates.length; i++) {{
        #         const face = faceCoordinates[i];
        #         console.log(face)
        #         if (x >= face.left && x <= face.right && y >= face.top && y <= face.bottom) {{
        #             alert(face.details);  // Display face details in an alert
        #             break;
        #         }}
        #     }}
        # }});
        # </script>
        # """, unsafe_allow_html=True)
       
#        <area shape ="circle" coords ="90,58,3"
# onmouseover="writeText('The planet Mercury is very difficult to study from the Earth because it is always so close to the Sun.')"
#  target ="_blank" alt="Mercury" />

# <area shape ="circle" coords ="124,58,8"
# onmouseover="writeText('Until the 1960s, Venus was often considered a twin sister to the Earth because Venus is the nearest planet to us, and because the two planets seem to share many characteristics.')"
#  target ="_blank" alt="Venus" />


# Define your javascript
        my_js = """
        <img 
            src ="http://localhost:8502/media/98dddd6129238362d1125fa350e6ed27fde4ad39cf947ef35a54a066.jpg" width=704; alt="Planets" usemap="#planetmap" />
        <map name="planetmap">
<area shape ="rect" coords ="420.1147446632385,1141.6007533073425,2443.2676315307617,2957.313346862793
}"
href ="https://www.w3schools.com/js/sun.htm" target ="_blank" alt="Sun" />

</map>
<p id="desc" style="color='white'">Mouse over the sun and the planets and see the different descriptions.</p>
            <script language="javascript">
            function writeText(txt) {
  document.getElementById("desc").innerHTML = txt;
}
function onHover(){console.log('mouse location:')}

addEventListener("mousemove", (event) => {
    let data = DATA
    console.log("event", event)
    let x = event.clientX;
    let y = event.clientY;
console.log(data.filter((ele) => {
  return x >= ele?.left && x <= ele?.right && y >= ele?.top && y <= ele?.bottom
}))
    });

</script>
<style>
p{
color: white;
}
iframe[Attributes Style] {
    width: 704px;
    height: 100vh !important;
}
<style>

        """.replace("DATA", str(test))

        # Wrapt the javascript as html code
        # my_html = f"<script>{my_js}</script>"

        # Execute your app
        st.title("Javascript example")
    #     st.markdown("""<style> .element-container:has(iframe[height="100vh"]) {display: block; } </style> """,
    # unsafe_allow_html=True)
        components.html(my_js,height=1000)
