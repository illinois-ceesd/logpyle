def setup():
    from jinja2 import Environment, FileSystemLoader
    import HTMLalyzer
    import logpyle
    import os
    import base64

    print("Building HTML file for HTMLalyzer!")

    with open("HTMLalyzer/logpyle-2023.2.3-py2.py3-none-any.whl", "rb") as f:
        binary_data = f.read()
        data = base64.b64encode(binary_data)
        logpyleWhlFileString = data.decode("utf-8")

    with open("HTMLalyzer/pymbolic-2022.2-py3-none-any.whl", "rb") as f:
        binary_data = f.read()
        data = base64.b64encode(binary_data)
        pymbolicWhlFileString = data.decode("utf-8")

    html_path = os.path.dirname(HTMLalyzer.__file__)
    logpyle_path = os.path.dirname(logpyle.__file__)
    enviroment = Environment(loader=FileSystemLoader(html_path+"/templates/"))
    template = enviroment.get_template("index.html")

    newFileHTML = open(html_path+"/templates/newFile.html", "r").read()
    mainPy = open(html_path+"/main.py", "r").read()
    mainPy = Environment().from_string(mainPy)
    mainPy = mainPy.render(
            newFileHTML=newFileHTML,
            logpyleWhlFileString=logpyleWhlFileString,
            pymbolicWhlFileString=pymbolicWhlFileString,
            )
    mainCSS = open(html_path+"/main.css", "r").read()
    mainJs = open(html_path+"/main.js", "r").read()

    # os.system("python3 -m build --wheel -o HTMLalyzer/")

    content = template.render(
            cssFile=mainCSS,
            pythonFile=mainPy,
            jsFile=mainJs,
            )

    filename = "web-interface.html"
    with open(html_path+'/'+filename, mode="w", encoding='utf-8') as message:
        message.write(content)

    print("HTML file build successfully!!!")

if __name__ == "__main__":
    setup()
