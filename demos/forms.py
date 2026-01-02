from django import forms
from .models import SukubunData

class SukubunForm(forms.ModelForm):
    class Meta:
        model = SukubunData
        fields = ["file", "note"]