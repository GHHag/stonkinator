import pickle


def serialize_models(models):
    binary_models = {}
    for instrument, model in models.items():
        binary_models[instrument] = pickle.dumps(model)
    return binary_models


if __name__ == '__main__':
    pass
