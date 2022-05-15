import os
import pickle
import warnings

def pickable(obj: any) -> bool:
    try:    pickle.loads(pickle.dumps(obj))
    except: return False

    return True

class PickableObject (object):
    unrestored_attrs = []

    @staticmethod
    def restore(fpath: str, raise_warning: bool = True) -> object:
        # Restores the object using the given filepath
        with open(fpath, "rb") as handle:
            restored_object = pickle.load(handle)

        if not isinstance(restored_object, PickableObject):
            return restored_object

        restored_object.restore_unpickable_attrs(fpath)
        restored_object.unrestored_attrs = [
            name for name in restored_object.unrestored_attrs
            if not hasattr(restored_object, name)
        ]

        if restored_object.unrestored_attrs and raise_warning:
            warnings.warn("\nFollowing attributes were not restored: " +
                    f"{restored_object.unrestored_attrs}.")

        return restored_object

    def __init__(self, **kwargs) -> None:
        for kw, arg in kwargs:
            setattr(self, kw, arg)

    def __getstate__(self) -> dict:
        # Returns a pickable state
        state = self.__dict__.copy()
        state["unrestored_attrs"] = [] # Reset unrestored state

        # Remove unpickable objects from state
        for name, attr in state.items():
            if not pickable(attr):
                state["unrestored_attrs"].append(name)
        
        for name in state["unrestored_attrs"]:
            state.pop(name)

        return state

    def save(self, fpath: str) -> None:
        # Saves this object to the filepath
        dpath = os.path.dirname(fpath)

        if dpath != '' and not os.path.exists(dpath):
            os.makedirs(dpath)

        with open(fpath, "wb") as handle:
            pickle.dump(self, handle, protocol=pickle.HIGHEST_PROTOCOL)

        self.save_unpickable_attrs(fpath)

    def save_unpickable_attrs(self, fpath: str) -> None:
        pass

    def restore_unpickable_attrs(self, fpath: str) -> None:
        pass

if __name__ == "__main__":
    pass
