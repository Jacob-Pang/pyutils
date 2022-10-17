#pragma once

class Resource {
    private:
        unsigned int capacity = 1;
        unsigned int usage = 0;
    
    public:
        Resource();
        Resource(const unsigned int capacity);

        virtual int getTimeToNextUpdate() const;

        virtual void update();
        virtual bool hasFreeCapacity(int units);
        virtual void use(int units);
        virtual void free(int units);
};
