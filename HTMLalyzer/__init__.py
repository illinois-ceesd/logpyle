def setup():
    from jinja2 import Environment, FileSystemLoader
    import HTMLalyzer
    import logpyle
    import os

    print("Building HTML file for HTMLalyzer!")

    html_path = os.path.dirname(HTMLalyzer.__file__)
    logpyle_path = os.path.dirname(logpyle.__file__)
    enviroment = Environment(loader=FileSystemLoader(html_path+"/templates/"))
    template = enviroment.get_template("index.html")

    newFileHTML = open(html_path+"/templates/newFile.html", "r").read()
    mainPy = open(html_path+"/main.py", "r").read()
    mainPy = Environment().from_string(mainPy)
    mainPy = mainPy.render(newFileHTML=newFileHTML)
    mainCSS = open(html_path+"/main.css", "r").read()
    mainJs = open(html_path+"/main.js", "r").read()

    runalyzerFile = open(html_path+"/modifiedRunalyzer.py", "r").read()
    runalyzerGatherFile = open(html_path+"/modifiedRunalyzerGather.py", "r").read()
    logpyleFile  = open(html_path+"/modifiedLogpyle.py", "r").read()


    content = template.render(
            cssFile=mainCSS,
            pythonFile=mainPy,
            runalyzerFile=runalyzerFile,
            logpyleFile=logpyleFile,
            runalyzerGatherFile=runalyzerGatherFile ,
            jsFile=mainJs,
            )

    filename = "web-interface.html"
    with open(html_path+'/'+filename, mode="w", encoding='utf-8') as message:
        message.write(content)

    print("HTML file build successfully!!!")

if __name__ == "__main__":
    setup()
