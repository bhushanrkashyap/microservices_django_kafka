from django.db import models

# Create your models here.
class Inventory(models.Model):
    product_name = models.CharField(max_length=255)
    quantity = models.IntegerField()
    
    class Meta:
        db_table = "inventory"