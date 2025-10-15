class SensitiveStr(str):
    def __new__(cls, value):
        return super().__new__(cls, value)
    
    def __repr__(self):
        return "<SensitiveStr: ****>"
    
    def __str__(self):
        return "****"
    
    def reveal(self):
        return super().__str__()