default: all

all: build

build:
	javac -cp .:./lib/* *.java

clean:
	rm *.class
	rm -rf data
	rm -rf azure
