<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body>

<form action="upload_file.php" method="post" enctype="multipart/form-data">
    <label for="file">image：</label>
    <input type="file" name="file" id="file"><br>
    <input type="submit" name="submit" value="submit">
</form>

<form action="upload_video.php" method="post" enctype="multipart/form-data">
    <label for="file">video：</label>
    <input type="file" name="file" id="file"><br>
    <input type="submit" name="submit" value="submit">
</form>

<form action="watermark.py" method="post" enctype="multipart/form-data">
    <label for="file">image：</label>
    <input type="file" name="file" id="file"><br>
    <input type="submit" name="submit" value="submit">
</form>

</body>
</html>