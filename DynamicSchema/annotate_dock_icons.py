import base64
import io
from PIL import Image, ImageDraw, ImageFont

def annotate_dock_icons(screenshot_path="latest_screenshot.png",
                       output_path="annotated_screenshot.png"):
    # Load the screenshot
    img = Image.open(screenshot_path).convert("RGBA")
    draw = ImageDraw.Draw(img)

    # If you want text labels, you'll need a font.
    try:
        font = ImageFont.truetype("/Library/Fonts/Arial.ttf", 14)
    except OSError:
        font = ImageFont.load_default()

    # Define bounding boxes and labels
    boxes = [
        ("Firefox",  (420, 715, 50, 50)),
        ("Chrome",   (480, 715, 50, 50)), 
        ("Settings", (540, 715, 50, 50)),
        ("Activity", (600, 715, 50, 50)),
        ("Preview",  (660, 715, 50, 50)),
    ]

    # Draw them
    for (label, (x, y, w, h)) in boxes:
        # Rectangle in red
        draw.rectangle([x, y, x + w, y + h], outline="red", width=3)
        # Label just above the box
        draw.text((x, y - 15), label, fill="red", font=font)

    # Save annotated image
    img.save(output_path)

if __name__ == "__main__":
    annotate_dock_icons()
