import numpy as np
import getProbs as gp

def gibbsSampling(iterations, docs, K, alpha, beta):
    N = len(docs)  
    V = len(docs[0])  

    pi = np.random.dirichlet(alpha * np.ones(K))  
    mu = np.random.dirichlet(beta * np.ones(V), K)  
    c = np.random.choice(K, N)  

    for _ in range(iterations):
        log_allMus = np.log(mu)

        
        for i in range(N):
            probs = gp.getProbs(True, docs[i], pi, log_allMus)
            c[i] = np.random.choice(K, p=probs)

        
        category_counts = np.bincount(c, minlength=K)
        pi = np.random.dirichlet(alpha + category_counts)

    
        for j in range(K):
            word_counts = np.sum(docs[c==j], axis=0)
            mu[j] = np.random.dirichlet(beta + word_counts)

    return pi, mu, c