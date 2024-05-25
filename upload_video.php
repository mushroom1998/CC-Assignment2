<?php
$allowedExts = array("mp4", "avi", "mov", "mkv");
$temp = explode(".", $_FILES["file"]["name"]);
$extension = end($temp);
if ((($_FILES["file"]["type"] == "video/mp4")
|| ($_FILES["file"]["type"] == "video/avi")
|| ($_FILES["file"]["type"] == "video/quicktime")
|| ($_FILES["file"]["type"] == "video/x-msvideo")
|| ($_FILES["file"]["type"] == "video/x-matroska"))
&& ($_FILES["file"]["size"] < 204800000) // 200MB limit, adjust as needed
&& in_array($extension, $allowedExts))
{
    if ($_FILES["file"]["error"] > 0)
    {
        echo "Error: " . $_FILES["file"]["error"] . "<br>";
    }
    else
    {
        echo "Uploaded filename: " . $_FILES["file"]["name"] . "<br>";
        echo "File type: " . $_FILES["file"]["type"] . "<br>";
        echo "File size: " . ($_FILES["file"]["size"] / 1024 / 1024) . " MB<br>";
        echo "Temporary storage location: " . $_FILES["file"]["tmp_name"] . "<br>";

        if (file_exists("upload/" . $_FILES["file"]["name"]))
        {
            echo $_FILES["file"]["name"] . " File already exists. ";
        }
        else
        {
            move_uploaded_file($_FILES["file"]["tmp_name"], "upload/" . $_FILES["file"]["name"]);
            echo "Storage location: " . "upload/" . $_FILES["file"]["name"];
        }
    }
}
else
{
    echo "Invalid form.";
}
?>
