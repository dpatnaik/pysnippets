class MyClass:
    def method(self):
        return 'Instance method called', self
    
    @classmethod
    def classmethod(cls):
        return 'class method called', cls
    
    @staticmethod
    def staticmethod():
        return 'static method called'