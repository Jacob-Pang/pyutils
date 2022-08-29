import inspect

def get_overridden_methods(base_class: type, instance: object) -> set:
    shared_methods = set(dir(base_class)) & set(dir(instance.__class__))
    shared_methods = {
        method for method in shared_methods
        if not method.startswith("__")
    }
    
    return {
        method for method in shared_methods
        if getattr(base_class, method) != getattr(instance.__class__, method)
    }

class WrapperMetaclass (type):
    def __new__(cls, name, bases, attrs, **kwargs) -> None:
        return type.__new__(cls, name, bases, attrs)

    def __init__(self, name, bases, attr, wrapped_class: type) -> None:
        def get_attr_proxy(name):
            def attr_proxy(self, *args):
                return getattr(self.wrapped_object, name)
            
            return attr_proxy

        type.__init__(self, name, bases, attr)
        assert hasattr(self, "wrapped_object")

        overridden_methods = get_overridden_methods(wrapped_class, self).union({
                "__class__",
                "__dict__",
                "__mro__",
                "__new__",
                "__init__",
                "__setattr__",
                "__getattr__",
                "__getattribute__"
            })

        for attr_name in dir(wrapped_class):
            if attr_name in overridden_methods:
                continue

            setattr(self, attr_name, property(get_attr_proxy(attr_name)))

class WrappedFunction:
    @staticmethod
    def get_compat_kwargs(method: callable, **kwargs) -> dict:
        # Returns the set of kwargs that are compatible with <method>.
        if not kwargs: return kwargs

        keywords = [
            *inspect.getfullargspec(method)[0],  # args
            *inspect.getfullargspec(method)[4]   # kwonlyargs
        ]

        return {kw: arg for kw, arg in kwargs.items() if kw in keywords}

    def __init__(self, target_function: callable, *args: any, **kwargs: any):
        self.target_function = target_function
        self.args = args
        self.kwargs = WrappedFunction.get_compat_kwargs(target_function, **kwargs)

    def set_kwargs(self, kwargs: dict) -> dict:
        for kw, arg in self.kwargs.items():
            if kw in kwargs:
                continue

            kwargs[kw] = arg # override

        return WrappedFunction.get_compat_kwargs(self.target_function, **kwargs)

    def __call__(self, *args, **kwargs) -> any:
        # Uses args if passed otherwise uses args passed on __init__.
        kwargs = self.set_kwargs(kwargs)

        return self.target_function(*args, **kwargs) if args else \
            self.target_function(*self.args, **kwargs)

if __name__ == "__main__":
    pass