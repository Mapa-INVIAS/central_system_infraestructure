from django.db import models

# Create your models here.
class sukubunData(models.Model):
    file = models.FileField(upload_to="uploads/")
    note = models.CharField(max_length=500)
    create_at = models.DateField(auto_now_add=True)
    update_at = models.DateField(auto_now=True)


    def __str__(self):
        return f"{self.file} - {self.note} - {self.create_at}"
    
     
