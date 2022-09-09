import numpy as np
from datetime import datetime
from copy import deepcopy

# ----- U T I L I T Y   F U N C T I O N S ----------------------------------- #


def _periodic_wrap(xs, xlimits):
    """Wrap list components values inside limits.

    Args:
        xs (list, N): input list for wrapping.
        xlimits (lits, Nx2): nested list of N [min, max] pairs representing
            bounds for the N elements on the input list.

    Returns:
        numpy.ndarray, N: array of values wrapped to limits.
        numpy.ndarray, N: array of booleans in which i-th element is True if
            i-th element of the input list has been wrapped, False otherwise.

    Example:
        >>> xs = [-2, 17, 8, 9, 2, 1]
        >>> xlimits = [[2,8]]*len(xs)
        >>> periodic_wrap(xs, xlimits)
            ([4, 5, 8, 3, 2, 7],
             [True, True, False, True, False, True])
    """
    new_xs = []
    did_wraps = []
    for x, xmin, xmax in zip(xs,
                             np.array(xlimits)[:,0],
                             np.array(xlimits)[:,1]):
        if (x < xmin) or (x > xmax):
            new_xs.append(xmin + (x-xmin)%(xmax-xmin))
            did_wraps.append(True)
        else:
            new_xs.append(x)
            did_wraps.append(False)

    return new_xs, did_wraps


def _accept_boltzmann(df, temp):
    """Classical annealing Boltzmann acceptance probability:
            min{1, exp(-df/temp)}
        This implementation avoids numerical warnings raised when np.exp
        yields np.inf (-df/temp values larger than ~1000).

    Args:
        df (float): cost function ("energy") difference.
        temp (float): artificial temperature.

    Returns:
        float: acceptance probability.
    """
    a = -1*df/temp
    if a >= 0:
        return 1
    else:
        return np.exp(a)


# ----- A N N E A L I N G   F U N C T I O N --------------------------------- #

def annealing(f, domain, domain_type,
              init_temp=1000, final_temp=1E-2, niter=1000, decay_type='exp',
              init_visit_scale=1000, min_visit_scale=1,
              f_kwargs={}, filename=None):
    """Simulated annealing for discrete or continuos cost functions.

    Annealing procedure is performed in niter steps, labeled by an index k,
    in which an input vector x is optimized with respect to a cost function f.
    Main components of the annealing procedure are:
        > Cost function - f(x)
        > Temperature profile - T(K)
        > Visiting function - visit:x->x_new
        > Acceptance probability - P
    This function uses a Boltzmann type probability of a new state x_new
        with respect to a current x state:
        P = min{ 1, exp((f(x)-f(x_new))/T) }
    Annealing iterations are such as follows:
        get starting x
        for k ranging from 0 to niter-1:
            T = T(k)
            get x_new
            if P(f, x, x_new, T) > random(0,1):
                x = x_new

    Args:
        f (function): Cost function minimized by annealing function.
            Must have the form f(x, **kwargs), in which x is a list.
        domain (list): Nested list defining minimization domain.
            if domain_type=='continuous', domain must have the form:
                    [[min_1, max_1], ..., [min_n, max_n]]
                In which min and max represent the coordinate limits in
                a given dimension of a n-dimensional space.
            If domain_type='discrete', domain must have the form:
                    [[op_1_1, ... op_m1_1], ..., [op_1_n, ..., op_mn_n]]
                In which [op_1_i, op_mi_i] contains the mi discrete options
                for the value of the i-th component of an n-dimensional vector.
        domain_type (str): defines wether the components of the x input vector
            for the const function are continuos or discrete. Possible values
            for the components are given by the domain argument (see above).
        init_temp (float, optional): Initial value for artificial temperature,
            in the same units as the cost function. Zero temperature is not
            allowed, use very small value instead. Defaults to 1000.
        final_temp (float, optional): Final value for artificial temperature,
            in the same units as the cost function. Zero temperature is not
            allowed, use very small value instead. Defaults to 1E-2.
        niter (int, optional): Number of annealing iterations performed. Each
            iteration contains one call to the cost function, in a new
            position that may or may not be accepted by the annealing
            algorithm. Defaults to 1000.
        decay_type (str, optional): Temperature decay function type.
            Available options are:
                'exp': exponential decay: T(k) = T(0)*exp(-a*k)
                'linear': linear decay: T(K) = T(0) + a*k
                With: T(0) = init_temp
                      T(niter-1) = final_temp
                Defaults to 'exp'.
        init_visit_scale (int, optional): Initial value for scale factor
            defining how far from the current input vector the visiting
            function may take the new input vector.
            if domain_type=='continuous', visiting function consists of a
                a normal distribution for each component centered at the
                current input vector.
                > In this case, the scale defines the standard deviation
                  at each component.
                That is:
                    Visiting function receives a
                        x = [..., xi, ...]
                    and yields:
                        x_new = [..., f(xi), ...]
                    In which f(xi) is taken from a normal distribution
                    centered in xi and with standard deviation =visit_scale.
            If domain_type='discrete', visiting function changes a random
                element of the current input vector option available at the
                dimension of the chosen random element.
                > I this case, the scale defines the number of times the
                  element swapping process.
                Example:
                  domain = [[a,b,c], [a,b,c], [u,v]]
                  x = [a, a, u]
                  visit_scale = 3 means 3 swap iterations:
                    [a, a, u] -> [b, a, u] -> [b, a, v] -> [b, a, v]
                  x_new = [b, a, v]
            Defaults to 1000.
        min_visit_scale (int, optional): Minimum value for visiting scale
            (see init_visit_scale for details on visiting scale meaning).
            Scale decay follow the temperature decay:
                visit_scale = min {
                                   (T/T(0))*init_visit_scale,
                                   min_visit_scale
                                   }
        f_kwargs (dict, optional): Additional keyword arguments dictionary
            passed to f cost function. Defaults to {}.
        filename (str, optional): If this argument is provided, an output
            file will be written with its name containing detailed information
            on the annealing procedure, including input vector at each step.
            Defaults to None.

    Raises:
        ValueError: If decay type is not 'exp' nor 'linear'.

    Returns:
        dict: Dictionary containing optimized input vector and corresponding
            cost function value, keyed by 'x' and 'fun', respectively.
    """
    # Space dimensionality.
    dim = len(domain)

    # Check decay_type input.
    if decay_type not in ['exp', 'linear']:
        raise ValueError('Wrong decay type (should be "exp" or "linear"')
    # Zero temperature leads to numerical problems (log(0) or division by 0).
    if min(final_temp, init_temp) < 0.99999*np.finfo(float).resolution:
        ValueError('Minimum accepted temperature is: {:.1e}'.format(
                                                np.finfo(float).resolution))

    # Exponential decay factor for achieving final_temp in niter steps.
    if decay_type == 'exp':
        decay_fac = np.log(final_temp/init_temp)*(1/(niter-1))

    # Generate initial input vector.
    if domain_type == 'continuous':
        x = [np.random.uniform(bound[0], bound[1]) for bound in domain]
    elif domain_type == 'discrete':
        x = [np.random.choice(options) for options in domain]
    x = np.array(x)
    # Calculate objective function value for initial input vector.
    fun = f(x, **f_kwargs)

    # Write header. File output has further columns for vector components and
    # is delimited by ' ', while printed columns avoid printing long vectors
    # and are aligned for imporving readability.
    print('{:7s}{:12s}{:12s}{:12s}{:8s}{:8s}{:8s}{:8s}'.format(
            'k', 'temp', 'fun', 'fun_new', 'p', 'rn', 'accept', 'time'))
    if filename is not None:
        output_file = open(filename, 'w')
        header = 'k temp fun {:s} fun_new {:s} visit_scale p rn accept time\n'\
                .format(' '.join([f'x{i:d}' for i in range(dim)]),
                        ' '.join([f'x_new{i:d}' for i in range(dim)]))
        output_file.write(header)

    # Annealing cycle.
    for k in range(niter):

        # Get temperature for current k-th step.
        if decay_type == 'linear':
            # Obs: the linear equation bellow is written in such a way that
            #      it avoids subtracting a small number from a large number.
            #      Ex: if init_temp=100 and final_temp=1e-15, python would
            #          yield 100 for init_temp-final_temp, what is not right.
            temp = init_temp - (k/(niter-1))*init_temp \
                             + (k/(niter-1))*final_temp
        elif decay_type == 'exp':
            temp = init_temp*np.exp(decay_fac*k)
        # Get visiting function scale for current k-th step.
        visit_scale = init_visit_scale*(temp/init_temp)
        visit_scale = max(visit_scale, min_visit_scale)

        # Get candidate new input vector.
        if domain_type == 'continuous':
            x_new = x + np.random.normal(scale=visit_scale, size=dim)
            x_new, _ = _periodic_wrap(x_new, domain)
        elif domain_type == 'discrete':
            x_new = deepcopy(x)
            # Change a random element of x to a random element from its
            # corresponding list of options in domain. Do it visit_scale times.
            for _ in range(round(visit_scale)):
                index = np.random.choice(range(dim))
                state = np.random.choice(domain[index])
                x_new[index] = state

        # Calculate objective function value from candidate input vector.
        fun_new = f(x_new, **f_kwargs)
        df = fun_new - fun

        # Compare accept probability and 0-1 random number for determining
        # wether new candidate input vector will be accepted.
        p = _accept_boltzmann(df, temp)
        rn = np.random.random()
        accept = (p >= rn) # Accept switch.

        # Write iteration info.
        now_str = datetime.now().strftime('%y/%m/%d-%H:%M:%S')
        print('{:<7d}{:<12.3e}{:<12.3e}{:<12.3e}{:<8.4f}{:<8.4f}{:<8s}{:<23s}'\
                .format(k, temp, fun, fun_new, p, rn, str(accept), now_str))
        if filename is not None:
            fmt = ('{:d} {:.6f} {:.6f} ' + '{} '*dim +
                    '{:.6f} ' + ('{} '*dim) + '{:.6f} {:.6f} {:} {:s}')
            line = fmt.format(k, temp, fun, *x, fun_new, *x_new,
                                p, rn, accept, now_str)
            output_file.write(line + '\n')
            output_file.flush()

        # Update vector and function values.
        if accept:
            x = x_new
            fun = fun_new

    if filename is not None:
        output_file.close()

    return {'x':x, 'fun':fun}


# ----- T E S T S ----------------------------------------------------------- #


if __name__ == '__main__':

    # Rule of thumb:
    # init_temp ~ average f values in domain
    # final_temp ~ init_temp / 1e5
    # init_visit_scale ~ 10 * average continuous domain size in a dimension.
    #                  ~ average number of discrete options in a dimension.

    print('\nTEST 1: Discrete case: \n')

    def f(x):
        result = 0
        for element in x:
            if element == 'a':
                result += 40
            elif element == 'b':
                result += 30
            elif element == 'c':
                result += 20
            elif element == 'd':
                result += 10
        return result

    discrete_case = annealing(f,
                              domain=[['a', 'b', 'c', 'd']]*5,
                              domain_type='discrete',
                              init_temp=125,
                              final_temp=1,#e-3,
                              niter=5000,
                              decay_type='exp',
                              init_visit_scale=5,
                              min_visit_scale=1,
                              f_kwargs={},
                              filename='test_discrete.txt')

    print('\nTEST 2: Continuos case case: \n')

    def f(x):
        x = np.array(x)
        return np.sum(x*x - 10*np.cos(2*np.pi*x)) + 10*np.size(x)

    continuos_case = annealing(f,
                                domain=[[-5, 5], [-5, 5]],
                                domain_type='continuous',
                                init_temp=37,
                                final_temp=4,#e-4,
                                niter=5000,
                                decay_type='exp',
                                init_visit_scale=100,
                                min_visit_scale=0.01,
                                f_kwargs={},
                                filename='test_continuos.txt')

    print('\nRESULT 1: Discrete case:')
    print(discrete_case)
    print('expected: f("d", "d", "d", "d", "d") = 50')
    print('\nRESULT 2: Continuos case:')
    print(continuos_case)
    print('expected: f(0,0) = 0')
