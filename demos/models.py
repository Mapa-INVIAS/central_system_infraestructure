import os
from django.core.files.storage import FileSystemStorage
from django.db import models


class OverwriteStorage(FileSystemStorage):
    def get_available_name(self, name, max_length = None):
        if self.exists(name):
            os.remove(os.path.join(self.location, name))
        return name

def sukubun_path(instance, filename):
    ext = filename.split('.')[-1]
    return f"uploads/sukubun.{ext}"

# Create your models here.
class SukubunData(models.Model):
    # file = models.FileField(upload_to="uploads/")
    file = models.FileField(upload_to=sukubun_path, storage=OverwriteStorage())
    note = models.CharField(max_length=500)
    create_at = models.DateField(auto_now_add=True)
    update_at = models.DateField(auto_now_add=True)


    def __str__(self):
        return f"{self.file} - {self.note} - {self.create_at}"
     
