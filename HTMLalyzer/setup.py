def setup():
    from jinja2 import Environment, FileSystemLoader
    import os
    import sys

    enviroment = Environment(loader=FileSystemLoader(os.path.join(sys.path[0],"templates/")))
    template = enviroment.get_template("index.html")

    filename = "web-interface.html"
    newFileHTML = open(os.path.join(sys.path[0],"templates/newFile.html"), "r").read()
    mainPy = open(os.path.join(sys.path[0],"main.py"), "r").read()
    mainPy = Environment().from_string(mainPy)
    mainPy = mainPy.render(newFileHTML=newFileHTML)
    mainCSS = open(os.path.join(sys.path[0],"main.css"), "r").read()
    runalyzerFile = open(os.path.join(sys.path[0],"modifiedRunalyzer.py"), "r").read()

    content = template.render(
            cssFile=mainCSS,
            pythonFile=mainPy,
            runalyzerFile=runalyzerFile,
            )
    with open(filename, mode="w", encoding='utf-8') as message:
        message.write(content)

if __name__ == "__main__":
    setup()
