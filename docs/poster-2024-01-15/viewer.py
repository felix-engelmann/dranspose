req = requests.get("http://pipeline-reducer/api/v1/result/$")
result = pickle.loads(req.content)
pos = list(result[0]["map"].keys())
plt.scatter([x[0] for x in pos],[x[1] for x in pos])
