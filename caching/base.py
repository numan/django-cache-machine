import functools
import logging

from django.conf import settings
from django.core.cache import cache, parse_backend_uri
from django.db import models
from django.db.models import signals
from django.db.models.sql import query
from django.db.models.sql.where import WhereNode, Constraint
from django.utils import encoding

from .invalidation import invalidator, flush_key, model_flush_key, make_key, byid

from datetime import timedelta
second_delta = timedelta(seconds=1)

class NullHandler(logging.Handler):

    def emit(self, record):
        pass


log = logging.getLogger('caching')
log.addHandler(NullHandler())

FOREVER = 0
NO_CACHE = -1
CACHE_PREFIX = getattr(settings, 'CACHE_PREFIX', '')
FETCH_BY_ID = getattr(settings, 'FETCH_BY_ID', False)
CACHE_DEBUG = getattr(settings, 'CACHE_DEBUG', False)

class CachingManager(models.Manager):

    # Tell Django to use this manager when resolving foreign keys.
    use_for_related_fields = True

    def get_query_set(self):
        return CachingQuerySet(self.model)

    def contribute_to_class(self, cls, name):
        signals.pre_save.connect(self.pre_save, sender=cls)
        signals.post_save.connect(self.post_save, sender=cls)
        signals.post_delete.connect(self.post_delete, sender=cls)
        signals.m2m_changed.connect(self.m2m_changed)
        return super(CachingManager, self).contribute_to_class(cls, name)

    def pre_save(self, sender, instance, raw, **kwargs):
        """
        Flush all cached queries associated with a model if a field has been
        changed that is a known constraint in an already cached query.
        
        TODO: Associate flush lists with constraint columns, so that we don't
        need to flush the whole table.
        """
        # The raw boolean means we're loading the database from a fixture, so
        # we don't want to mess with it.
        if raw:
            return

        # We only need to flush the model if the post already exists; when new
        # instances are created it flushes the model cache, so calling flush
        # here would be redundant
        if not instance.id:
            return
        
        cls = instance.__class__
        if not hasattr(cls.objects, 'invalidate_model'):
            return
        
        # Grab the original object, before the to-be-saved changes
        orig = cls.objects.no_cache().get(pk=instance.id)
        
        constraint_key = 'cols:%s' % instance.model_key
        flush_cols = invalidator.get_flush_lists([constraint_key])
        if len(flush_cols) == 0:
            return
        
        for col in flush_cols:
            if not hasattr(orig, col) or not hasattr(instance, col):
                continue
            if getattr(orig, col) != getattr(instance, col):
                cls.objects.invalidate_model()
                return

    def post_save(self, instance, created, **kwargs):
        self.invalidate(instance)
        if created:
            invalidator.clear()
        
    def post_delete(self, instance, **kwargs):
        self.invalidate(instance)
    
    def m2m_changed(self, instance, action, *args, **kwargs):
        if action[:4] != "post":
            return
        self.invalidate(instance)
    
    def invalidate(self, *objects):
        keys = []
        for o in objects:
            if hasattr(o, '_cache_keys'):
                keys += list(o._cache_keys())
        """Invalidate all the flush lists associated with ``objects``."""
        if len(keys) > 0:
            invalidator.invalidate_keys(keys)

    def invalidate_model(self):
        """
        Invalidate all the flush lists associated with the models of ``objects``.
        
        This effectively flushes all queries linked to a given model.
        """
        model_key = self.model._model_key()
        if CACHE_DEBUG:
            log.debug("Invalidating model %s" % model_key[2:])
        if not hasattr(self.model, '_model_key'):
            raise Exception((
                "The model Manager of %s uses caching, but the " + \
                "model does not. Needs CachingMixIn."
            ) % ".".join([self.model.__module__, self.model.__name__]))
        invalidator.invalidate_keys([model_key])
        cache.delete(u'cols:%s' % model_key)

    def raw(self, raw_query, params=None, *args, **kwargs):
        return CachingRawQuerySet(raw_query, self.model, params=params,
                                  using=self._db, *args, **kwargs)

    def cache(self, timeout=None):
        return self.get_query_set().cache(timeout)

    def no_cache(self):
        return self.cache(NO_CACHE)


class CacheMachine(object):
    """
    Handles all the cache management for a QuerySet.

    Takes the string representation of a query and a function that can be
    called to get an iterator over some database results.
    """

    def __init__(self, query_string, iter_function, timeout=None):
        self.query_string = query_string
        self.iter_function = iter_function
        self.timeout = timeout

    def query_key(self):
        """Generate the cache key for this query."""
        key = make_key('qs:%s' % self.query_string, with_locale=False)
        cache.set('sql:%s' % key, self.query_string)
        return key

    def __iter__(self):
        try:
            query_key = self.query_key()
        except query.EmptyResultSet:
            raise StopIteration

        # Try to fetch from the cache.
        cached = cache.get(query_key)
        if cached is not None:
            if CACHE_DEBUG:
                log.debug('cache hit: %s' % self.query_string)
            for obj in cached:
                obj.from_cache = True
                yield obj
            return

        # Do the database query, cache it once we have all the objects.
        iterator = self.iter_function()

        to_cache = []
        try:
            while True:
                obj = iterator.next()
                obj.from_cache = False
                to_cache.append(obj)
                yield obj
        except StopIteration:
            if to_cache:
                self.cache_objects(to_cache)
            raise

    def cache_objects(self, objects):
        """Cache query_key => objects, then update the flush lists."""
        query_key = self.query_key()
        query_flush = flush_key(self.query_string)
        cache.add(query_key, objects, timeout=self.timeout)
        invalidator.cache_objects(objects, query_key, query_flush)

class CachingQuerySet(models.query.QuerySet):

    def __init__(self, *args, **kw):
        super(CachingQuerySet, self).__init__(*args, **kw)
        self.timeout = None

    def flush_key(self):
        return flush_key(self.query_key())

    def query_key(self):
        sql, params = self.query.get_compiler(using=self.db).as_sql()
        return sql % params
    
    def get_constraints(self):
        """
        Get the table/column constraints associated with the queryset's query.
        
        TODO: Look at join information.
        """
        constraints = {}
        stack = [self.query.where]
        while stack:
            curr_where = stack.pop()
            for k, v in curr_where.__dict__.items():
                if isinstance(v, (list, tuple)):
                    for i, item in enumerate(v):
                        if isinstance(item, WhereNode):
                            stack.append(item)
                        elif isinstance(item, (tuple)):
                            if len(item) > 0 and isinstance(item[0], Constraint):
                                constraint = item[0]
                                model = constraint.field.model
                                name = constraint.field.name
                                if not hasattr(model, '_model_key'):
                                    continue
                                # If the primary key, don't add to list
                                if model._meta.pk and model._meta.pk.name == name:
                                    continue
                                constraint_key = u'cols:%s' % model._model_key()
                                if constraint_key not in constraints:
                                    constraints[constraint_key] = set()
                                if name not in constraints[constraint_key]:
                                    constraints[constraint_key].add(name)
        return constraints


    def iterator(self):
        constraints = self.get_constraints()
        invalidator.add_to_flush_list(constraints)
        iterator = super(CachingQuerySet, self).iterator
        if self.timeout == NO_CACHE:
            return iter(iterator())
        else:
            try:
                # Work-around for Django #12717.
                query_string = self.query_key()
            except query.EmptyResultSet:
                return iterator()
            if FETCH_BY_ID:
                iterator = self.fetch_by_id
            return iter(CacheMachine(query_string, iterator, self.timeout))

    def fetch_by_id(self):
        """
        Run two queries to get objects: one for the ids, one for id__in=ids.

        After getting ids from the first query we can try cache.get_many to
        reuse objects we've already seen.  Then we fetch the remaining items
        from the db, and put those in the cache.  This prevents cache
        duplication.
        """
        # Include columns from extra since they could be used in the query's
        # order_by.
        vals = self.values_list('pk', *self.query.extra.keys())
        pks = [val[0] for val in vals]
        keys = dict((byid(self.model._cache_key(pk)), pk) for pk in pks)
        cached = dict((k, v) for k, v in cache.get_many(keys).items()
                      if v is not None)

        # Pick up the objects we missed.
        missed = [pk for key, pk in keys.items() if key not in cached]
        if missed:
            others = self.fetch_missed(missed)
            # Put the fetched objects back in cache.
            new = dict((byid(o), o) for o in others)
            cache.set_many(new)
        else:
            new = {}

        # Use pks to return the objects in the correct order.
        objects = dict((o.pk, o) for o in cached.values() + new.values())
        for pk in pks:
            yield objects[pk]

    def fetch_missed(self, pks):
        # Reuse the queryset but get a clean query.
        others = self.all()
        others.query.clear_limits()
        # Clear out the default ordering since we order based on the query.
        others = others.order_by().filter(pk__in=pks)
        if hasattr(others, 'no_cache'):
            others = others.no_cache()
        if self.query.select_related:
            others.dup_select_related(self)
        return others

    def count(self):
        timeout = getattr(settings, 'CACHE_COUNT_TIMEOUT', None)
        super_count = super(CachingQuerySet, self).count
        query_string = 'count:%s' % self.query_key()
        if timeout is None:
            return super_count()
        else:
            return cached_with(self, super_count, query_string, timeout)

    def cache(self, timeout=None):
        qs = self._clone()
        qs.timeout = timeout
        return qs

    def no_cache(self):
        return self.cache(NO_CACHE)

    def _clone(self, *args, **kw):
        qs = super(CachingQuerySet, self)._clone(*args, **kw)
        qs.timeout = self.timeout
        return qs


class CachingMixin:
    """Inherit from this class to get caching and invalidation helpers."""

    def flush_key(self):
        return flush_key(self)

    @property
    def cache_key(self):
        """Return a cache key based on the object's primary key."""
        return self._cache_key(self.pk)

    @classmethod
    def _cache_key(cls, pk):
        """
        Return a string that uniquely identifies the object.

        For the Addon class, with a pk of 2, we get "o:addons.addon:2".
        """
        key_parts = ('o', cls._meta, pk)
        return ':'.join(map(encoding.smart_unicode, key_parts))

    def model_flush_key(self):
        return model_flush_key(self.model_key)

    @property
    def model_key(self):
        """Returns a cache key based on the object's model."""
        return self._model_key()

    @classmethod
    def _model_key(cls):
        """
        Return a string that uniquely identifies the model the object
        belongs to.
        
        For the Addon class, we get "m:addons.addon".
        """
        key_parts = ('m', cls._meta)
        return ':'.join(map(encoding.smart_unicode, key_parts))

    def _model_keys(self):
        """
        Return the model cache key for self plus all related foreign keys.
        """
        return (self.model_key,) + self._cache_keys()[1:]

    def _cache_keys(self):
        """Return the cache key for self plus all related foreign keys."""
        fks = dict((f, getattr(self, f.attname)) for f in self._meta.fields
                    if isinstance(f, models.ForeignKey))

        keys = [fk.rel.to._cache_key(val) for fk, val in fks.items()
                if val is not None and hasattr(fk.rel.to, '_cache_key')]
        return (self.cache_key,) + tuple(keys)


class CachingRawQuerySet(models.query.RawQuerySet):

    def __iter__(self):
        iterator = super(CachingRawQuerySet, self).__iter__
        sql = self.raw_query % tuple(self.params)
        for obj in CacheMachine(sql, iterator):
            yield obj
        raise StopIteration


def _function_cache_key(key):
    return make_key('f:%s' % key, with_locale=True)


def cached(function, key_, duration=None):
    """Only calls the function if ``key`` is not already in the cache."""
    key = _function_cache_key(key_)
    val = cache.get(key)
    if val is None:
        if CACHE_DEBUG:
            log.debug('cache miss for %s' % key)
        val = function()
        cache.set(key, val, duration)
    elif CACHE_DEBUG:
        log.debug('cache hit for %s' % key)
    return val


def cached_with(obj, f, f_key, timeout=None):
    """Helper for caching a function call within an object's flush list."""
    try:
        obj_key = (obj.query_key() if hasattr(obj, 'query_key')
                   else obj.cache_key)
    except AttributeError:
        log.warning(u'%r cannot be cached.' % obj)
        return f()

    key = '%s:%s' % tuple(map(encoding.smart_str, (f_key, obj_key)))
    # Put the key generated in cached() into this object's flush list.
    invalidator.add_to_flush_list(
        {obj.flush_key(): [_function_cache_key(key)]})
    return cached(f, key, timeout)


class cached_method(object):
    """
    Decorator to cache a method call in this object's flush list.

    The external cache will only be used once per (instance, args).  After that
    a local cache on the object will be used.

    Lifted from werkzeug.
    """
    def __init__(self, func):
        self.func = func
        functools.update_wrapper(self, func)

    def __get__(self, obj, type=None):
        if obj is None:
            return self
        _missing = object()
        value = obj.__dict__.get(self.__name__, _missing)
        if value is _missing:
            w = MethodWrapper(obj, self.func)
            obj.__dict__[self.__name__] = w
            return w
        return value


class MethodWrapper(object):
    """
    Wraps around an object's method for two-level caching.

    The first call for a set of (args, kwargs) will use an external cache.
    After that, an object-local dict cache will be used.
    """
    def __init__(self, obj, func):
        self.obj = obj
        self.func = func
        functools.update_wrapper(self, func)
        self.cache = {}

    def __call__(self, *args, **kwargs):
        k = lambda o: o.cache_key if hasattr(o, 'cache_key') else o
        arg_keys = map(k, args)
        kwarg_keys = [(key, k(val)) for key, val in kwargs.items()]
        key = 'm:%s:%s:%s:%s' % (self.obj.cache_key, self.func.__name__,
                                 arg_keys, kwarg_keys)
        if key not in self.cache:
            f = functools.partial(self.func, self.obj, *args, **kwargs)
            self.cache[key] = cached_with(self.obj, f, key)
        return self.cache[key]
