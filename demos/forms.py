from django import forms
from .models import sukubunData

class sukubunForm(forms.ModelForm):
    class Meta:
        model = sukubunData
        fields = ["file", "note"]