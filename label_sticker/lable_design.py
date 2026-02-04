import math
from PIL import Image, ImageDraw

# ขนาดภาพ cm
# Define dimensions in cm
w = 5.2
h = 3.075
dpi = 96  # Define DPI (dots per inch)

# Convert cm to pixels
w_px = int(w * dpi / 2.54)
h_px = int(h * dpi / 2.54)
px = int(dpi / 2.54)  

# Define shape in pixels
shape = [((w_px//2)+(px*0.1), (h_px//2)-px), (px, px)]

# Creating new Image object
img = Image.new("RGB", (w_px, h_px), "white")

# Create rectangle image
img1 = ImageDraw.Draw(img)
img1.rectangle(shape, fill="white", outline="black")
img.show()