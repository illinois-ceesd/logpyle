def setup():
    from jinja2 import Environment, FileSystemLoader
    import HTMLalyzer
    import os

    print("Building HTML file for HTMLalyzer!")

    path = os.path.dirname(HTMLalyzer.__file__)
    enviroment = Environment(loader=FileSystemLoader(path+"/templates/"))
    template = enviroment.get_template("index.html")

    newFileHTML = open(path+"/templates/newFile.html", "r").read()
    mainPy = open(path+"/main.py", "r").read()
    mainPy = Environment().from_string(mainPy)
    mainPy = mainPy.render(newFileHTML=newFileHTML)
    mainCSS = open(path+"/main.css", "r").read()
    runalyzerFile = open(path+"/modifiedRunalyzer.py", "r").read()

    content = template.render(
            cssFile=mainCSS,
            pythonFile=mainPy,
            runalyzerFile=runalyzerFile,
            )

    filename = "web-interface.html"
    with open(path+'/'+filename, mode="w", encoding='utf-8') as message:
        message.write(content)

    print("HTML file build successfully!!!")

if __name__ == "__main__":
    setup()
